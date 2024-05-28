import sys
sys.path.append('C:\\Users\\Nsokolov\\Bootcamp\\debootcamp-chessapi')
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import logging
import yaml
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, BigInteger, DATE, TIMESTAMP, Boolean



from assets.Chess import extract_eco_codes, extract_games, extract_user_info, load, incremental_modify_dates, transform
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

    # get config file
    yaml_file_path = __file__.replace(".py", ".yaml")
    if Path(yaml_file_path).exists():
        with open(yaml_file_path) as yaml_file:
            pipeline_config = yaml.safe_load(yaml_file)
            PIPLINE_NAME = pipeline_config.get("name")
    else:
        raise Exception(
            f"Missing {yaml_file_path} file! Please create the yaml file with at least a `name` key for the pipeline name."
        )

    # extracting variables from config file
    chess_api_client = ChessApiClient(pipeline_config.get("config").get("games").get("username"), user_agent=USER_AGENT)
    start_date = pipeline_config.get("config").get("games").get("start_date")
    end_date = pipeline_config.get("config").get("games").get("end_date")
    target_table_games = pipeline_config.get("config").get("games").get("target_table")
    target_column = pipeline_config.get("config").get("games").get("target_column")
    
    players = pipeline_config.get("config").get("players").get("usernames")
    target_table_players = pipeline_config.get("config").get("players").get("target_table")


    # defining postrgesql client
    postgres_sql_client = PostgreSqlClient(server_name=SERVER_NAME,
                    database_name=DATABASE_NAME,
                    username=DB_USERNAME,
                    password=DB_PASSWORD,
                    port=PORT)
    metadata = MetaData()

    # TODO - add check for the availability of the postgres instance


    # extract

    # run this "incremental_modify_dates" function to check if the username exists, if so the start date will update to one day ahead of max date
    # end date will evaluate to current date
    start_date, end_date = incremental_modify_dates(ChessApiClient=chess_api_client,
                                                    PostgreSqlClient=postgres_sql_client,
                                                    target_table=target_table_games,
                                                    target_column=target_column,
                                                    start_date=start_date,
                                                    end_date=end_date)
    valid_games = extract_games(start_date=start_date,
                  end_date=end_date,
                  chess_api_client=chess_api_client)
    eco_codes = extract_eco_codes('./assets/data/eco_codes.csv')
    if valid_games.shape[0] > 0:
        #transform
        trasformed_games = transform(valid_games, eco_codes)
        print(trasformed_games.head())
        #load
        games_tbl = Table("games",
            metadata,
            Column('game_id',BigInteger, primary_key=True),
            Column('game_url', String),
            Column('game_mode', String),
            Column('start_date', String),
            Column('username', String),
            Column('user_color', String),
            Column('user_rating', Integer),
            Column('user_accuracy', Float),
            Column('opponent', String),
            Column('opponent_rating', Integer),
            Column('opponent_accuracy', Float),
            Column('rating_diff', Integer),
            Column('match_result', String),
            Column('result_subcategory', String),
			Column('start_date_time', String),
            Column('end_date_time', String),
            Column('game_duration', String),
            Column('game_duration_sec', Integer),
            Column('rounds', Integer),
			Column('user_avg_move_time_sec', Float),
            Column('opening', String)
            )
        load(df=trasformed_games,
            postgresql_client=postgres_sql_client,
            table=games_tbl,
            metadata=metadata,
            load_method="overwrite")

    # players 
    
    # defining target table
    players_tbl = Table("players",
        metadata,
        Column('player_id', BigInteger, primary_key=True),
        Column('snaphot_date', TIMESTAMP, default=datetime.now(), primary_key=True),
        Column('name',String),
        Column('username', String),
        Column('title', String),
        Column('followers', BigInteger),
        Column('country', String),
        Column('location', String),
        Column('last_online', BigInteger),
        Column('joined', BigInteger),
        Column('is_streamer', Boolean)
    )

    # looping through players in config file
    for username in players:

        chess_api_client = ChessApiClient(username, user_agent=USER_AGENT)

        # extract player info
        player_df = extract_user_info(chess_api_client=chess_api_client)

        # transform player (adding missing columns if needed)
        player_df = player_df.reindex(columns=['player_id',
                               'name',
                               'username',
                               'title',
                               'followers',
                               'country',
                               'location',
                               'last_online',
                               'joined',
                               'is_streamer'])

        #load player
        load(df=player_df,
            postgresql_client=postgres_sql_client,
            table=players_tbl,
            metadata=metadata,
            load_method="insert"
        )




