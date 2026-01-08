# import schedule
import time
import pandas as pd
import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine
import db_constants
import alert

connection_string = f'postgresql+psycopg2://{db_constants.DB_USER}:{db_constants.DB_PASSWORD}@{db_constants.DB_HOST}:{db_constants.DB_PORT}/{db_constants.DB_NAME}'
# engine = create_engine(connection_string)

def execute_query(query):
    conn = psycopg2.connect(dbname=db_constants.DB_NAME, user=db_constants.DB_USER, password=db_constants.DB_PASSWORD, host=db_constants.DB_HOST, port='5432', sslmode='require')
    cursor = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)
    print(cursor)
    print(query)

    try:
        # Execute the SQL query
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
    except psycopg2.DatabaseError as e:
        # Rollback the transaction in case of an error
        # alert.send_notif(message='【DB Error】\nIn Fund Balance Data >> update_db >> execute_query\n'+str(e), chat_id='-4675914050') # api error group
        conn.rollback()
        print(f"An error occurred: {e}")


def update_balance_data(df, source):
    engine = create_engine(connection_string)
    delete_query = f'DELETE FROM fund_balance_data WHERE source in ({source});'
    execute_query(delete_query)

    try:
        df.to_sql('fund_balance_data', engine, if_exists='append', index=False)
        print(f"Updated balances for {source}")
    except Exception as e:
        # alert.send_notif(message='【DB Error】\nIn Fund Balance Data >> update_db >> update_balance_data\n'+str(e), chat_id='-4675914050') # api error group
        print(f'Error encountered when updating fund_balance_data for {source}', e)
    
    engine.dispose()

def get_db_table(query):
    engine = create_engine(connection_string)
    try:
        df = pd.read_sql(query, engine)
        engine.dispose()
        return df
    except Exception as e:
        # alert.send_notif(message='【DB Error】\nIn Fund Balance Data >> update_db >> get_db_table\n'+str(e), chat_id='-4675914050') # api error group
        print(f'Error encountered getting sql table with this query {query}: ', e)
        engine.dispose()
        return pd.DataFrame()
    

def df_to_table(table_name, df):
    engine = create_engine(connection_string)
    if df.empty:
        return
    try:
        df.to_sql(table_name, engine, if_exists='append', index=False)
    except Exception as e:
        # alert.send_notif(message='【DB Error】\nIn Fund Balance Data >> update_db >> df_to_table\n'+str(e), chat_id='-4675914050') # api error group
        print(f'Error encountered when updating {table_name}', e)
    
    engine.dispose()

def df_replace_table(table_name, df):
    engine = create_engine(connection_string)
    if df.empty:
        return
    try:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    except Exception as e:
        # alert.send_notif(message='【DB Error】\n'+str(e), chat_id='-4675914050') # api error group
        print(f'Error encountered when updating {table_name}', e)
    
    engine.dispose()