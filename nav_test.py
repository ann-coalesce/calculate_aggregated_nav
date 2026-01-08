import json
from datetime import datetime, timedelta, timezone
import db_utils
import pandas as pd
import credentials
import time
import sheet_utils
import telegram


def get_fallback_balance_data(pm, curr_timestamp, max_lookback_hours=2):
    """
    Get the most recent valid balance data for a PM within the lookback period
    """
    lookback_start = curr_timestamp - timedelta(hours=max_lookback_hours)
    
    query = f'''
        SELECT 
            timestamp, 
            pm, 
            balance 
        FROM 
            balance_all_consolidated
        WHERE 
            pm = '{pm}' 
            AND timestamp >= '{lookback_start}'
            AND timestamp <= '{curr_timestamp}'
            AND balance IS NOT NULL
        ORDER BY 
            timestamp DESC
        LIMIT 1;
    '''
    
    fallback_data = db_utils.get_db_table(query=query)
    # return pd.DataFrame()
    return fallback_data

def get_pm_status_from_config(pm, pm_data_list):
    """
    Check if a PM is active based on the PM_DATA configuration
    Returns: 'active', 'inactive', or 'not_found'
    """
    for pm_info in pm_data_list:
        if pm_info['pm'] == pm:
            if pm_info.get('active', False):  # Default to True if 'active' key doesn't exist
                return 'active'
            else:
                return 'inactive'
    
    return 'not_found'  # PM not in configuration

def validate_and_enhance_balance_data(balance_df, curr_timestamp, pm_data_list):
    """
    Validate balance data and handle missing PMs based on their active status
    - Inactive PMs: Use current data if available, but NO fallback if missing
    - Active PMs: Use current data or fallback data if missing
    """
    # Create sets of PMs based on their active status
    active_pms = {pm_info['pm'] for pm_info in pm_data_list if pm_info.get('active', True)}
    inactive_pms = {pm_info['pm'] for pm_info in pm_data_list if not pm_info.get('active', True)}
    all_expected_pms = active_pms | inactive_pms
    
    # Get PMs that actually have current data
    actual_pms = set(balance_df['pm'].unique())
    
    # Initialize validation tracking
    validation_log = {
        'timestamp': curr_timestamp,
        'active_pms': {
            'expected_count': len(active_pms),
            'with_current_data': list(active_pms & actual_pms),
            'using_fallback_data': [],
            'completely_missing': []
        },
        'inactive_pms': {
            'expected_count': len(inactive_pms),
            'with_current_data': list(inactive_pms & actual_pms),
            'missing_data': list(inactive_pms - actual_pms)
        },
        'summary': {
            'total_expected_pms': len(all_expected_pms),
            'total_with_data': len(actual_pms)
        }
    }
    
    # Handle missing active PMs - try fallback data
    missing_active_pms = active_pms - actual_pms
    fallback_data_list = []
    
    for missing_pm in missing_active_pms:
        print(f"Active PM missing data: {missing_pm}, attempting fallback...")
        
        fallback_data = get_fallback_balance_data(missing_pm, curr_timestamp)
        
        if not fallback_data.empty:
            fallback_data['timestamp'] = pd.to_datetime(fallback_data['timestamp'])
            original_timestamp = fallback_data.iloc[0]['timestamp']
            # Update timestamp to current for aggregation purposes
            fallback_data['timestamp'] = curr_timestamp
            fallback_data['is_fallback'] = True
            fallback_data['is_inactive'] = False
            fallback_data_list.append(fallback_data)
            
            validation_log['active_pms']['using_fallback_data'].append({
                'pm': missing_pm,
                'fallback_timestamp': original_timestamp,
                'balance': fallback_data.iloc[0]['balance']
            })
            print(f"Using fallback data for {missing_pm} from {original_timestamp}")
        else:
            validation_log['active_pms']['completely_missing'].append(missing_pm)
            print(f"No fallback data found for active PM {missing_pm}")
    
    # Handle missing inactive PMs - just log them
    for inactive_pm in validation_log['inactive_pms']['missing_data']:
        print(f"PM {inactive_pm} is inactive and missing data - skipping (no fallback)")
    
    # Mark existing data as fallback or not, and active/inactive
    enhanced_balance = balance_df.copy()
    enhanced_balance['is_fallback'] = False
    enhanced_balance['is_inactive'] = enhanced_balance['pm'].isin(inactive_pms)
    
    # Add fallback data for active PMs only
    if fallback_data_list:
        fallback_combined = pd.concat(fallback_data_list, ignore_index=True)
        enhanced_balance = pd.concat([enhanced_balance, fallback_combined], ignore_index=True)
    
    return enhanced_balance, validation_log

def main():
    try:
        # ----- for per minute update last minute's aggregated NAV ----
        curr = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
        curr_hour = curr.replace(minute=0,second=0, microsecond=0)
        prev_hour = curr_hour - timedelta(hours=1)

        query = 'select * from shares_table;'
        shares = db_utils.get_db_table(query=query)
        shares['timestamp'] = pd.to_datetime(shares['timestamp'])
        latest_shares = shares.sort_values(by='timestamp', ascending=False).drop_duplicates(subset='pm')
        # print(latest_shares)

        grouping_df = pd.DataFrame(credentials.PM_DATA)
        # print(grouping_df)

        query = f'''SELECT 
            timestamp, 
            pm, 
            balance AS balance 
        FROM 
            balance_all_consolidated
        WHERE 
            timestamp = '{curr_hour}' OR timestamp = '{curr}'
        ORDER BY 
            timestamp;'''

        balance = db_utils.get_db_table(query=query)
        balance['timestamp'] = pd.to_datetime(balance['timestamp'])
        balance = balance.loc[balance.groupby('pm')['timestamp'].idxmax()]
        # print("Original balance data:")
        # print(balance)

        # # --- TESTING FALLBACK LOGIC ---
        # # Drop the latest row for pm 'cta_alphamike' to force fallback
        # if "cta_alphamike" in balance['pm'].values:
        #     latest_idx = balance.loc[balance['pm'] == 'cta_alphamike', 'timestamp'].idxmax()
        #     balance = balance.drop(latest_idx)
        #     print("\n[TEST] Dropped latest row for pm cta_alphamike to trigger fallback.\n")

        # ===== NEW VALIDATION AND FALLBACK LOGIC =====
        balance_enhanced, validation_log = validate_and_enhance_balance_data(
            balance, curr, credentials.PM_DATA
        )
        
        print("\nValidation Log:")
        print(json.dumps(validation_log, indent=2, default=str))
        
        # print("\nEnhanced balance data (with fallbacks):")
        # print(balance_enhanced)
        
        # Log validation results to database (optional)
        validation_df = pd.DataFrame([validation_log])
        # db_utils.df_to_table(table_name='nav_validation_log', df=validation_df)
        
        # ===== END VALIDATION LOGIC =====

        balance_merged = pd.merge(balance_enhanced, grouping_df, on='pm', how='left')
        # print('balance_merged')
        # print(balance_merged)

        # Add fallback and inactive indicators to final results
        pm_grouped = balance_merged.groupby(by=['timestamp','pm_group']).agg({
            'balance': 'sum',
            'is_fallback': 'any',  # True if any component used fallback data
            'is_inactive': 'any'   # True if any component was inactive
        }).reset_index()
        # print('pm_grouped')
        # print(pm_grouped)
        pm_grouped.rename(columns={'pm_group':'pm'}, inplace=True)

        group_grouped = balance_merged.groupby(by=['timestamp','group']).agg({
            'balance': 'sum',
            'is_fallback': 'any',
            'is_inactive': 'any'
        }).reset_index()
        # print('group_grouped')
        # print(group_grouped)
        group_grouped.rename(columns={'group':'pm'}, inplace=True)

        fund_grouped = balance_merged.groupby(by=['fund']).agg({
            'balance': 'sum', 
            'timestamp': 'max',
            'is_fallback': 'any',
            'is_inactive': 'any'
        }).reset_index()
        fund_grouped.rename(columns={'fund':'pm'}, inplace=True)
        
        # Handle duplicated rows for gross calculations
        duplicated_rows = fund_grouped[fund_grouped['pm'] == 'sp1'].copy()
        duplicated_rows['pm'] = 'sp1-gross'

        duplicated_rows_2 = fund_grouped[fund_grouped['pm'] == 'sp2'].copy()
        duplicated_rows_2['pm'] = 'sp2-gross'

        duplicated_rows_3 = fund_grouped[fund_grouped['pm'] == 'sp2-classb'].copy()
        duplicated_rows_3['pm'] = 'sp2-classb-gross'

        duplicated_rows_4 = fund_grouped[fund_grouped['pm'] == 'sp3'].copy()
        duplicated_rows_4['pm'] = 'sp3-gross'

        duplicated_rows_5 = fund_grouped[fund_grouped['pm'] == 'sp2-classa'].copy()
        duplicated_rows_5['pm'] = 'sp2-classa-gross'

        fund_grouped = pd.concat([fund_grouped, duplicated_rows, duplicated_rows_2, duplicated_rows_3, duplicated_rows_4, duplicated_rows_5])
        # print('fund_grouped')
        # print(fund_grouped)

        bal_concat = pd.concat([pm_grouped, group_grouped, fund_grouped])

        pm_result_df = pd.merge(bal_concat, latest_shares[['pm','shares']], on='pm', how='left')
        pm_result_df['nav'] = pm_result_df.apply(
            lambda row: 0 if pd.isna(row['shares']) or row['shares'] == 0 else row['balance'] / row['shares'],
            axis=1
        )
        
        pm_result_df.dropna(inplace=True)
        
        if curr.minute != 0:
            print('not including herm, fof if not hour 00')
            pm_result_df = pm_result_df[~pm_result_df['pm'].isin(['sp1-fof-tangoecho', 'sp1-fof-hermeneutic', 'sp1-fof', 'sp1-cash-cash', 'sp1-cash','sp1-fof-northrock', 'sp1-fof-defiance', 'sp2-cash-cash', 'sp2-cash', 'sp3-cash-cash', 'sp3-cash', 'sp2-classb-cash-cash','sp2-classb-cash'])]
        
        pm_result_df = pm_result_df[pm_result_df['pm'] != 'sp1-sma-robinfunding']
        pm_result_df = pm_result_df[~pm_result_df['pm'].isin(['sp2', 'sp2-gross', 'sp2-sma', 'sp2-sma-romeo'])] 

        print('Final pm_result_df with fallback and inactive indicators:')
        print(pm_result_df)

        # URL = 'https://docs.google.com/spreadsheets/d/1RDA5hceXI4KOqAWJgdu8E_Rp0VueWfkCQEZemtcYUqY/edit?gid=0#gid=0'
        # sheet_name = 'Sheet3'
        # sheet_utils.set_dataframe(df=pm_result_df, sheet_name=sheet_name, url=URL)
        # Save results
        df_db = pm_result_df.copy()
        df_db = df_db[['timestamp', 'pm', 'balance', 'shares', 'nav', 'is_fallback']]
        db_utils.df_to_table(table_name='nav_table', df=df_db)

        # ===== ENHANCED REPORTING =====
        active_info = validation_log['active_pms']
        inactive_info = validation_log['inactive_pms']
        
        print("\n=== DATA VALIDATION REPORT ===")
        
        # Active PMs status
        print(f"ACTIVE PMs ({active_info['expected_count']} expected):")
        print(f"  ✓ With current data: {len(active_info['with_current_data'])} PMs")
        if active_info['with_current_data']:
            print(f"    {active_info['with_current_data']}")
            
        if active_info['using_fallback_data']:
            print(f"  ⚠ Using fallback data: {len(active_info['using_fallback_data'])} PMs")
            
            # Format telegram message for fallback data usage
            fallback_msg = f"⚠️ NAV AGGREGATION ALERT ⚠️\n\n"
            fallback_msg += f"{len(active_info['using_fallback_data'])} active PMs using fallback data:\n"
            for fallback_info in active_info['using_fallback_data']:
                fallback_msg += f"• {fallback_info['pm']}: from {fallback_info['fallback_timestamp']}\n"
                print(f"    - {fallback_info['pm']}: from {fallback_info['fallback_timestamp']}")
            fallback_msg += f"\n⚠️ Check data pipeline immediately!"
            
            # Send telegram notification
            try:
                telegram.send_notif(fallback_msg)
            except Exception as e:
                print(f"Failed to send fallback telegram notification: {e}")
                
        if active_info['completely_missing']:
            print(f"  ❌ Completely missing: {len(active_info['completely_missing'])} PMs")
            print(f"    {active_info['completely_missing']}")
            
            # Format telegram message for completely missing data
            missing_msg = f"🚨 CRITICAL NAV AGGREGATION ALERT 🚨\n\n"
            missing_msg += f"{len(active_info['completely_missing'])} active PMs have NO DATA:\n"
            for pm in active_info['completely_missing']:
                missing_msg += f"• {pm}\n"
            missing_msg += f"\n🚨 URGENT: These PMs have no current or fallback data!"
            
            # Send telegram notification
            try:
                telegram.send_notif(missing_msg)
            except Exception as e:
                print(f"Failed to send missing data telegram notification: {e}")
        
        
        # Inactive PMs status
        # print(f"\nINACTIVE PMs ({inactive_info['expected_count']} expected):")
        if inactive_info['with_current_data']:
            print(f"  ✓ With current data: {len(inactive_info['with_current_data'])} PMs")
            print(f"    {inactive_info['with_current_data']}")
        # if inactive_info['missing_data']:
        #     print(f"  - Missing data (expected): {len(inactive_info['missing_data'])} PMs")
        #     print(f"    {inactive_info['missing_data']}")
        
        # Overall summary
        total_processed = (len(active_info['with_current_data']) + 
                         len(active_info['using_fallback_data']) + 
                         len(inactive_info['with_current_data']))
        
        print(f"\nSUMMARY:")
        print(f"  Total PMs processed: {total_processed}/{validation_log['summary']['total_expected_pms']}")
        print(f"  Data quality: {'✓ GOOD' if not active_info['completely_missing'] else '⚠ NEEDS ATTENTION'}")
        
        # Issue-based alerts
        if active_info['using_fallback_data']:
            print(f"\n🔔 ALERT: {len(active_info['using_fallback_data'])} active PMs using fallback data - check data pipeline")
        
        if active_info['completely_missing']:
            print(f"\n🚨 CRITICAL: {len(active_info['completely_missing'])} active PMs have no data available")
        
        print("=" * 35)
        print("completed")

    except Exception as e:
        print(f"Error in main: {e}")


if __name__ == '__main__':
    main()