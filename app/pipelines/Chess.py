import sys
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
import logging
import yaml
from sqlalchemy import Table, MetaData, Column, Integer, String, Float, BigInteger, DATE, TIMESTAMP, Boolean
from jinja2 import Environment, FileSystemLoader

from assets.Chess import extract_eco_codes, extract_games, extract_user_info, load, incremental_modify_dates, transform as transform_etl
from connectors.Chess import ChessApiClient
from connectors.postgresql import PostgreSqlClient
from assets.pipeline_logging import PipelineLogging
from assets.metadata_logging import MetaDataLoggingStatus, MetaDataLogging
from assets.extract_load_transform import (
    extract_load,
    transform,
    SqlTransform,
)
from graphlib import TopologicalSorter


if __name__ == "__main__":
    # setting up environment variables
    load_dotenv()
    LOGGING_SERVER_NAME = os.environ.get("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.environ.get("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.environ.get("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.environ.get("LOGGING_PASSWORD")
    LOGGING_PORT = os.environ.get("LOGGING_PORT")

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

    # defining logger
    pipeline_logging = PipelineLogging(
        pipeline_name=pipeline_config.get("name"),
        log_folder_path=pipeline_config.get('config').get("log_folder_path"),
    )

    #defining postgres sql for logging storage
    postgresql_logging_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    metadata_logger = MetaDataLogging(
        pipeline_name=PIPLINE_NAME,
        postgresql_client=postgresql_logging_client,
        config=pipeline_config.get('config'),
    )
    try:
        metadata_logger.log()
        # extracting variables from config file
        pipeline_logging.logger.info("Starting pipeline run")
        pipeline_logging.logger.info("Getting pipeline environment variables")
        USER_AGENT = os.environ.get("USER_AGENT")
        SERVER_NAME = os.environ.get("SERVER_NAME")
        DATABASE_NAME = os.environ.get("DATABASE_NAME")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        PORT = os.environ.get("PORT")
        start_date = pipeline_config.get("config").get("games").get("start_date")
        end_date = pipeline_config.get("config").get("games").get("end_date")
        target_table_games = pipeline_config.get("config").get("games").get("target_table")
        target_column = pipeline_config.get("config").get("games").get("target_column")
        usernames = pipeline_config.get("config").get("games").get("usernames")

        # extracting players from config, either from players section or from games section (if players section is missing/empty)
        players = pipeline_config.get("config").get("players").get("usernames")
        if players is None:
            players = pipeline_config.get("config").get("games").get("username")

        target_table_players = pipeline_config.get("config").get("players").get("target_table")


        # defining postrgesql client
        postgres_sql_client = PostgreSqlClient(server_name=SERVER_NAME,
                        database_name=DATABASE_NAME,
                        username=DB_USERNAME,
                        password=DB_PASSWORD,
                        port=PORT)
        metadata = MetaData()

        # TODO - add check for the availability of the postgres instance

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
        eco_codes = extract_eco_codes(pipeline_config.get("config").get("eco_codes_path"))
        pipeline_logging.logger.info('Begining Games ETL')
        for username in usernames:
            chess_api_client = ChessApiClient(username, USER_AGENT)
            # run this "incremental_modify_dates" function to check if the username exists, if so the start date will update to one day ahead of max date
            # end date will evaluate to current date
            start_date, end_date = incremental_modify_dates(ChessApiClient=chess_api_client,
                                                            PostgreSqlClient=postgres_sql_client,
                                                            target_table=target_table_games,
                                                            target_column=target_column,
                                                            start_date=start_date,
                                                            end_date=end_date)
            # extract
            pipeline_logging.logger.info(f'Extracting data from Chess API games: username: {chess_api_client.username}, start_date: {start_date}, end_date: {end_date}')
            valid_games = extract_games(start_date=start_date,
                        end_date=end_date,
                        chess_api_client=chess_api_client)
            if valid_games.shape[0] > 0:
                #transform
                pipeline_logging.logger.info('Trasforming dataframes')
                trasformed_games = transform_etl(valid_games, eco_codes)
                #load
                pipeline_logging.logger.info('Loading data to postgres')
                load(df=trasformed_games,
                    postgresql_client=postgres_sql_client,
                    table=games_tbl,
                    metadata=metadata,
                    load_method="upsert")
        pipeline_logging.logger.info('Games ETL run successful')
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
        # making sure players is a list to iretare through
        if not isinstance(players, list):
            players = [players]
        pipeline_logging.logger.info('Begining players ETL')
        for username in players:

            chess_api_client = ChessApiClient(username, user_agent=USER_AGENT)

            # extract player info
            pipeline_logging.logger.info(f'Extracting data from Chess API users: username: {chess_api_client.username}')
            player_df = extract_user_info(chess_api_client=chess_api_client)

            # transform player (adding missing columns if needed)
            pipeline_logging.logger.info('Trasforming dataframes')
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
            pipeline_logging.logger.info('Loading data to postgres')
            load(df=player_df,
                postgresql_client=postgres_sql_client,
                table=players_tbl,
                metadata=metadata,
                load_method="insert"
            )
            pipeline_logging.logger.info("Loading data to postgres")
        pipeline_logging.logger.info("Players ETL run successful")


        # Adding ELT Pipeline

        SOURCE_DATABASE_NAME = os.environ.get("SOURCE_DATABASE_NAME")
        SOURCE_SERVER_NAME = os.environ.get("SOURCE_SERVER_NAME")
        SOURCE_DB_USERNAME = os.environ.get("SOURCE_DB_USERNAME")
        SOURCE_DB_PASSWORD = os.environ.get("SOURCE_DB_PASSWORD")
        SOURCE_PORT = os.environ.get("SOURCE_PORT")
        TARGET_DATABASE_NAME = os.environ.get("TARGET_DATABASE_NAME")
        TARGET_SERVER_NAME = os.environ.get("TARGET_SERVER_NAME")
        TARGET_DB_USERNAME = os.environ.get("TARGET_DB_USERNAME")
        TARGET_DB_PASSWORD = os.environ.get("TARGET_DB_PASSWORD")
        TARGET_PORT = os.environ.get("TARGET_PORT")

        pipeline_logging.logger.info(f'Begining ELT')
        source_postgresql_client = PostgreSqlClient(
            server_name=SOURCE_SERVER_NAME,
            database_name=SOURCE_DATABASE_NAME,
            username=SOURCE_DB_USERNAME,
            password=SOURCE_DB_PASSWORD,
            port=SOURCE_PORT,
        )

        target_postgresql_client = PostgreSqlClient(
            server_name=TARGET_SERVER_NAME,
            database_name=TARGET_DATABASE_NAME,
            username=TARGET_DB_USERNAME,
            password=TARGET_DB_PASSWORD,
            port=TARGET_PORT,
        )

        extract_template_environment = Environment(
            loader=FileSystemLoader(pipeline_config.get("config").get("extract_template_path"))
        )

        # extract_load(
        #     template_environment=extract_template_environment,
        #     source_postgresql_client=source_postgresql_client,
        #     target_postgresql_client=target_postgresql_client,
        # )

        transform_template_environment = Environment(
            loader=FileSystemLoader(pipeline_config.get("config").get("transform_template_path"))
        )

        # create nodes
        performance = SqlTransform(
            table_name="performance",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )
        overall_performance = SqlTransform(
            table_name="overall_performance",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )
        top_openings = SqlTransform(
            table_name="top_openings",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )
        play_rating_trend = SqlTransform(
            table_name="play_rating_trend",
            postgresql_client=target_postgresql_client,
            environment=transform_template_environment,
        )
        # create DAG
        dag = TopologicalSorter()
        dag.add(performance)
        dag.add(overall_performance)
        dag.add(play_rating_trend)
        pipeline_logging.logger.info("Perform transform")
        transform(dag=dag)
        pipeline_logging.logger.info("Pipeline complete")
        # performance.create_table_as()
        # overall_performance.create_table_as()
        # play_rating_trend.create_table_as()

         # log end
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS, logs=pipeline_logging.get_logs()
        )
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"Pipeline run failed. See detailed logs: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE, logs=pipeline_logging.get_logs()
    )
        pipeline_logging.logger.handlers.clear()







