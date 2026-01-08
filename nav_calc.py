import json
from datetime import datetime, timedelta, timezone
import db_utils
import pandas as pd
import credentials
import time

def main():
    try:
        # ----- for per minute update last minute's aggregated NAV ----
        curr = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(minutes=1)
        # curr = datetime.now(timezone.utc).replace(second=0, microsecond=0) 
        curr_hour = curr.replace(minute=0,second=0, microsecond=0)
        prev_hour = curr_hour - timedelta(hours=1)

        query = 'select * from shares_table;'
        shares = db_utils.get_db_table(query=query)
        shares['timestamp'] = pd.to_datetime(shares['timestamp'])
        latest_shares = shares.sort_values(by='timestamp', ascending=False).drop_duplicates(subset='pm')
        print(latest_shares)

        grouping_df = pd.DataFrame(credentials.PM_DATA)
        print(grouping_df)

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

        # WHERE 
        #     (pm IN ('hermeneutic', 'defiance', 'northrock') AND timestamp = '{curr_hour}')
        #     OR 
        #     (pm NOT IN ('hermeneutic', 'defiance', 'northrock') AND timestamp = '{curr}')

        balance = db_utils.get_db_table(query=query)
        balance['timestamp'] = pd.to_datetime(balance['timestamp'])
        balance = balance.loc[balance.groupby('pm')['timestamp'].idxmax()]
        print(balance)

        balance_merged = pd.merge(balance, grouping_df, on='pm', how='left')
        print('balance_merged')
        print(balance_merged)

        pm_grouped = balance_merged.groupby(by=['timestamp','pm_group']).aggregate({'balance':'sum'}).reset_index()
        print('pm_grouped')
        print(pm_grouped)
        pm_grouped.rename(columns={'pm_group':'pm'}, inplace=True)

        group_grouped = balance_merged.groupby(by=['timestamp','group']).aggregate({'balance':'sum'}).reset_index()
        print('group_grouped')
        print(group_grouped)
        group_grouped.rename(columns={'group':'pm'}, inplace=True)

        fund_grouped = balance_merged.groupby(by=['fund']).aggregate({'balance':'sum', 'timestamp':'max'}).reset_index()
        fund_grouped.rename(columns={'fund':'pm'}, inplace=True)
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
        print('fund_grouped')
        print(fund_grouped)

        bal_concat = pd.concat([pm_grouped, group_grouped, fund_grouped])

        pm_result_df = pd.merge(bal_concat, latest_shares[['pm','shares']], on='pm', how='left')
        # pm_result_df['nav'] = pm_result_df['balance'] / pm_result_df['shares']
        pm_result_df['nav'] = pm_result_df.apply(
            lambda row: 0 if pd.isna(row['shares']) or row['shares'] == 0 else row['balance'] / row['shares'],
            axis=1
        )
        # sp1-disc, sp1-disc-deribitmaster not yet ready - they are dropped for now
        pm_result_df.dropna(inplace=True)
        # pm_result_df = pm_result_df[pm_result_df['pm'] not in ['sp1-fof-hermeneutic', 'sp1-fof', 'sp1-cash-cash', 'sp1-cash']]
        if curr.minute != 0:
            print('not including herm, fof if not hour 00')
            pm_result_df = pm_result_df[~pm_result_df['pm'].isin(['sp1-fof-tangoecho', 'sp1-fof-hermeneutic', 'sp1-fof', 'sp1-cash-cash', 'sp1-cash','sp1-fof-northrock', 'sp1-fof-defiance', 'sp2-cash-cash', 'sp2-cash', 'sp3-cash-cash', 'sp3-cash', 'sp2-classb-cash-cash','sp2-classb-cash'])]
        
        pm_result_df = pm_result_df[pm_result_df['pm'] != 'sp1-sma-robinfunding']

        print('pm_result_df')
        pm_result_df = pm_result_df[~pm_result_df['pm'].isin(['sp2', 'sp2-gross', 'sp2-sma', 'sp2-sma-romeo'])] 

        print(pm_result_df)

        db_utils.df_to_table(table_name='nav_table', df=pm_result_df)

        print("completed")

    except Exception as e:
        print(e)

    

def test():
    query = 'select * from nav_table order by timestamp desc limit 100;'
    df = db_utils.get_db_table(query)
    print(df)

if __name__ == '__main__':
    main()
# test()
# time.sleep(10)
# start = time.time()
# main()
# end = time.time()
# print("time taken", end-start)
