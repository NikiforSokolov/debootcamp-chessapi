import sys
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import logging
import yaml
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, BigInteger, DATE, TIMESTAMP



from assets.Chess import extract_eco_codes, extract_games, extract_user_info, load
from connectors.Chess import ChessApiClient
from connectors.postgresql import PostgreSqlClient

if __name__ == "__main__":
    # setting up environment variables
    load_dotenv()
    USER_AGENT = os.environ.get("USER_AGENT")
    SERVER_NAME = os.environ.get("SERVER_NAME")
    DATABASE_NAME = os.environ.get("DATABASE_NAME")
    DB_USERNAME = os.environ.get("DB_USERNAME")
    DB_PASSWORD = os.environ.get("DB_PASSWORD")
    PORT = os.environ.get("PORT")

    #get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPLINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    chess_api_client = ChessApiClient(pipeline_config.get("config").get("games").get("username"), user_agent=USER_AGENT)

    # extract
    postgres_sql_client = PostgreSqlClient(server_name=SERVER_NAME,
                     database_name=DATABASE_NAME,
                     username=DB_USERNAME,
                     password=DB_PASSWORD,
                     port=PORT)
    metadata = MetaData()
    games_df = extract_games(start_date=pipeline_config.get("config").get("games").get("start_date"),
                  end_date=pipeline_config.get("config").get("games").get("end_date"),
                  chess_api_client=chess_api_client)

    #transform
    games_df = games_df[['game_url',
                        'game_id',
                        'time_class',
                        'username',
                        'user_color',
                        'user_rating',
                        'opponent',
                        'opponent_rating',
                        'result',
                        'user_accuracy',
                        'opponent_accuracy',
                        'start_date',
                        'ECO',
                        'moves_per_player']]
    print(games_df.head())
    #load
    games_tbl = Table("games",
          metadata,
          Column('game_url',String),
          Column('game_id', BigInteger, primary_key=True),
          Column('time_class', String),
          Column('username', String),
          Column('user_color', String),
          Column('user_rating', Integer),
          Column('opponent', String),
          Column('opponent_rating', Integer),
          Column('result', String),
          Column('user_accuracy', Float),
          Column('opponent_accuracy', Float),
          Column('start_date', String),
          Column('ECO', String),
          Column('moves_per_player', Integer)
          )
    load(df=games_df,
         postgresql_client=postgres_sql_client,
         table=games_tbl,
         metadata=metadata,
         load_method="overwrite")

    # load(games_df,
    #      postgresql_client=postgres_sql_client,
    #      )
