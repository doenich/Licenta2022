
import sqlalchemy as sql
import pandas as pd
import os


def connect(server_address, database_name='master', driver='ODBC+Driver+17+for+SQL+Server', read_only=False):
    """
    Returns an sqlalchemy database connection.

    Input
    -----
    server_address - string containing server address
    database_name - string containing name of database
    read_only - boolean representing connection intent
    driver - string containing driver version

    Output
    ------
    > sqlalchemy driver
    """
    read_only = ';ApplicationIntent=ReadOnly' if read_only else ''
    connstring = 'mssql+pyodbc://{server_address}/{database_name}?trusted_connection=yes&driver={driver}'.format(
        server_address=server_address,
        database_name=database_name,
        driver=driver,
    )

    return sql.create_engine(connstring, connect_args={'App':'IntelligentCityManager{read_only}'.format(read_only=read_only)})



connection = connect("DESKTOP-DOUAE1J", "licenta")

def convert_dataframe(dataframe):

    dataframe = dataframe.drop(['user_id_str', 'thumbnail', 'geo', 'translate', 'trans_src', 'trans_dest'], axis=1)

    return dataframe

def write_folder_to_db(folder_name, table_name):
    for file in os.listdir(f'./data/{folder_name}'):
        if file.endswith('.csv'):
            # read the file
            print(f'> Reading {file}')
            try:
                data = pd.read_csv(f'./data/{folder_name}/{file}')
            except pd.errors.EmptyDataError:
                print(f'> {file} is empty')
                continue

            # convert the dataframe
            data = convert_dataframe(data)

            # write to database
            dtype = {
                'id': sql.types.BIGINT,
                'created_at': sql.types.DATETIME,
                'tweet_id': sql.types.BIGINT,
                'tweet_id_str': sql.types.VARCHAR(length=64),
                'tweet_text': sql.types.VARCHAR(length=1024),
                'tweet_lang': sql.types.VARCHAR(length=64),
                'tweet_coordinates': sql.types.VARCHAR(length=64),
                'tweet_place': sql.types.VARCHAR(length=64),
                'tweet_place_type': sql.types.VARCHAR(length=64),
                'tweet_place_country': sql.types.VARCHAR(length=64),
                'tweet_place_country_code': sql.types.VARCHAR(length=64),
                'conversation_id': sql.types.BIGINT,
                'created_at': sql.types.BIGINT,
                'date': sql.types.DATETIME,
                'timezone': sql.types.Integer,
                'place': sql.types.VARCHAR(length=256),
                'tweet': sql.types.VARCHAR(length=2048),
                'language': sql.types.VARCHAR(length=8),
                'hashtags': sql.types.VARCHAR(length=1028),
                'cashtags': sql.types.VARCHAR(length=256),
                'user_id': sql.types.BIGINT,
                'username': sql.types.VARCHAR(length=256),
                'name': sql.types.VARCHAR(length=512),
                'day': sql.types.Integer,
                'hour': sql.types.Integer,
                'link': sql.types.VARCHAR(length=512),
                'urls': sql.types.VARCHAR(length=2048),
                'photos': sql.types.VARCHAR(length=2048),
                'video': sql.types.BINARY,
                'retweet': sql.types.BINARY,
                'nlikes': sql.types.Integer,
                'nreplies': sql.types.Integer,
                'nretweets': sql.types.Integer,
                'quote_url': sql.types.VARCHAR(length=512),
                'search': sql.types.VARCHAR(length=512),
                'near': sql.types.VARCHAR(length=512),
                'source': sql.types.VARCHAR(length=512),
                'user_rt_id': sql.types.VARCHAR(length=512),
                'user_rt': sql.types.VARCHAR(length=512),
                'retweet_id': sql.types.VARCHAR(length=512),
                'reply_to': sql.types.VARCHAR(length=8000),
                'retweet_date': sql.types.VARCHAR(length=512),
            }

            print(f'> Writing {file} to {table_name}')
            data.to_sql(table_name, connection, if_exists='append', index=False, dtype=dtype)

            print(f'> {len(data.index)} rows from {file} written to {table_name}')

def main():
    """Entry point."""
    # read every file in /data/ folder
    write_folder_to_db('hashtags', 'raw_data')
    write_folder_to_db('content', 'raw_data')
    