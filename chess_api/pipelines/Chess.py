import sys
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import logging
import yaml
from sqlalchemy import Table, MetaData, Column, Integer, String, Float



from assets.Chess import extract_eco_codes, extract_games, extract_user_info, load
from connectors.Chess import ChessApiClient
from connectors.postgresql import PostgreSqlClient

if __name__ == "__main__":
    load_dotenv()
    USER_AGENT = os.environ.get("USER_AGENT")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    start_date = '2024-01-01'
    end_date = '2024-05-31'
    username = 'dolols'
    chess_api_client = ChessApiClient(username, user_agent=USER_AGENT)
    postgres_sql_client = PostgreSqlClient(server_name=SERVER_NAME,
                     database_name=DATABASE_NAME,
                     username=DB_USERNAME,
                     password=DB_PASSWORD,
                     port=PORT)
    metadata = MetaData()
    games_df = extract_games(start_date=start_date,
                  end_date=end_date,
                  chess_api_client=chess_api_client)
    # load(games_df,
    #      postgresql_client=postgres_sql_client,
    #      )
