import sys
from dotenv import load_dotenv
load_dotenv()

from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd
from sqlalchemy import Table, MetaData, Column, Integer, String, Float
from connectors.postgresql import PostgreSqlClient
import re
from connectors.Chess import ChessApiClient


def generate_monthly_dates(start_date: str, end_date: str) -> list[datetime]:
    """
    Generates a list of datetime objects with a monthly interval between the start_date and end_date.

    Parameters:
    - start_date (str): The start date in the format 'YYYY-MM-DD'.
    - end_date (str): The end date in the format 'YYYY-MM-DD'.

    Returns:
    - list: A list of datetime objects with a monthly interval. The first date in the list will be start_date,
            and the last date in the list will be end_date.

    Example:
    >>> generate_monthly_dates('2023-01-15', '2023-05-24')
    [datetime.datetime(2023, 1, 15, 0, 0),
     datetime.datetime(2023, 2, 15, 0, 0),
     datetime.datetime(2023, 3, 15, 0, 0),
     datetime.datetime(2023, 4, 15, 0, 0),
     datetime.datetime(2023, 5, 24, 0, 0)]

    Notes:
    - The input date strings must be in the format 'YYYY-MM-DD'.
    - If the end_date is not exactly one month after the last date in the generated list, the end_date will be updated
      to the list to ensure the final date is always the specified end_date.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # in case we just want to extract a single month we will return the start and end date without performing any iterations
    if start.year == end.year and start.month == end.month:
        return [start, end]

    dates = []
    current_date = start

    while current_date <= end:
        dates.append(current_date)
        current_date += relativedelta(months=1)

    # Ensure the last date is always the end_date
    if dates[-1] != end:
        dates[-1] = end

    return dates

def extract_games(start_date: str, end_date: str, chess_api_client: ChessApiClient) -> pd.DataFrame:

  months = generate_monthly_dates(start_date, end_date)
  valid_games = []
  start_date = months[0]
  end_date = months[-1]
  dates_ran = []
  for date in months:
      month_str = date.strftime('%Y-%m')
      year = date.year
      month = date.month
      if month_str not in dates_ran:
        dates_ran.append(month_str)
        games = chess_api_client.get_monthly_games(year=year, month=month)
        for game in games:
            parsed_game = parse_game(game, chess_api_client.username)
            if parsed_game is not None:
                game_date = datetime.strptime(parsed_game.get("start_date"),'%Y-%m-%d')
                if start_date <= game_date <= end_date:
                    valid_games.append(parsed_game)
  return pd.DataFrame(valid_games)

def incremental_modify_dates(ChessApiClient: ChessApiClient,
                             PostgreSqlClient: PostgreSqlClient,
                             target_table: str,
                             target_column: str,
                             start_date: str,
                             end_date: str) ->tuple[str]:
    """
    Modifies the start and end dates for an ETL process based on the latest game date for a specific username from Chess.com API.

    Parameters:
    ChessApiClient (ChessApiClient): An instance of the Chess API client containing the username.
    PostgreSqlClient (PostgreSqlClient): An instance of the PostgreSQL client managing database interactions.
    target_table (str): The name of the target table in the PostgreSQL database.
    target_column (str): The column name in the target table where the dates are stored.
    start_date (datetime): The initial start date for the ETL process.
    end_date (datetime): The initial end date for the ETL process.

    Returns:
    Tuple[datetime, datetime]: A tuple containing the modified start and end dates.

    Example:
    >>> from datetime import datetime
    >>> from dateutil.relativedelta import relativedelta
    >>> ChessApiClient.username = 'sample_user'
    >>> PostgreSqlClient.has_table = lambda table_name: True
    >>> PostgreSqlClient.engine.execute = lambda statement: [(datetime(2023, 1, 1),)]
    >>> start_date = datetime(2022, 1, 1)
    >>> end_date = datetime(2023, 1, 1)
    >>> new_start_date, new_end_date = incremental_modify_date(ChessApiClient, PostgreSqlClient, 'chess_games', 'game_date', start_date, end_date)
    >>> print(f"New Start Date: {new_start_date}")
    >>> print(f"New End Date: {new_end_date}")

    Notes:
    - The function assumes that the `target_table` and `target_column` exist in the PostgreSQL database.
    - If the username does not exist in the target table, the start and end dates remain unchanged.
    - The `end_date` is always set to the current date.
    """

    statement = f"""
                select max({target_column})
                from {target_table}
                where username='{ChessApiClient.username}'
                """
    if PostgreSqlClient.table_exists(table_name=target_table):
        max_value = PostgreSqlClient.engine.execute(statement).fetchall()[0][0]
        if max_value is not None:
            start_date = datetime.strptime(max_value,'%Y-%m-%d') + relativedelta(days=-2)
            end_date = datetime.now()
            start_date = start_date.strftime('%Y-%m-%d')
            end_date = end_date.strftime('%Y-%m-%d')
    return start_date, end_date

def extract_user_info(chess_api_client: ChessApiClient) -> pd.DataFrame:
    data = []
    data.append(chess_api_client.get_user_info())
    df = pd.DataFrame(data)
    return df

def pgn_to_dict(pgn: list) -> dict:
    pgn_dict = {}
    rows = pgn.split("\n")
    for row in rows:
        if row.find('[') == 0:
            element = row.strip('[]').split(' ', maxsplit=1)
            pgn_dict[element[0]] = element[1].strip('"')
        elif len(row)<1:
            continue
        else:
            moves = row
            moves = re.split(r' \d{1,}\. ',moves)
            pgn_dict['moves_count_per_player'] = len(moves)
            pgn_dict['moves_row'] = moves
    return pgn_dict

def parse_game(game: dict, username: str) -> dict:
    parsed_game = {}
    if game.get('pgn') is None:
        return
    parsed_game["game_url"] = game.get('url')
    parsed_game["pgn"] = game.get('pgn')
    parsed_game["game_id"] = re.search(r"(live|daily)\/(\d+)$",parsed_game["game_url"]).group(2)
    parsed_game["time_class"] = game.get("time_class")
    parsed_game['end_date_time'] = game.get("end_time")
    parsed_game["username"] = username

    if game.get("white").get("username").lower() == username or game.get("white").get("username") == username:
        parsed_game["user_color"] = "white"
        parsed_game["user_rating"] = game.get("white").get("rating")
        parsed_game["opponent"] = game.get("black").get("username")
        parsed_game["opponent_rating"] = game.get("black").get("rating")
        parsed_game["opponent_url"] = f"https://www.chess.com/member/{parsed_game['opponent']}"
        parsed_game["result"] = game.get("white").get("result")
        if game.get("accuracies") is not None:
            parsed_game["user_accuracy"] = game.get("accuracies").get("white")
            parsed_game["opponent_accuracy"] = game.get("accuracies").get("black")
        else:
            parsed_game["user_accuracy"] = None
            parsed_game["opponent_accuracy"] = None
    else:
        parsed_game["user_color"] = "black"
        parsed_game["user_rating"] = game.get("black").get("rating")
        parsed_game["opponent"] = game.get("white").get("username")
        parsed_game["opponent_rating"] = game.get("white").get("rating")
        parsed_game["opponent_url"] = f"https://www.chess.com/member/{parsed_game['opponent']}"
        parsed_game["result"] = game.get("black").get("result")
        if game.get("accuracies") is not None:
            parsed_game["user_accuracy"] = game.get("accuracies").get("black")
            parsed_game["opponent_accuracy"] = game.get("accuracies").get("white")
        else:
            parsed_game["user_accuracy"] = None
            parsed_game["opponent_accuracy"] = None
    parsed_pgn = pgn_to_dict(parsed_game["pgn"])
    parsed_game["pgn_result"] = parsed_pgn.get('Result')
    parsed_game["start_date"] = parsed_pgn.get("Date").replace('.','-')
    parsed_game["ECO"] = parsed_pgn.get("ECO")
    parsed_game["ECOUrl"] = parsed_pgn.get("ECOUrl")
    parsed_game["start_time"] = parsed_pgn.get("StartTime")
    parsed_game['moves_per_player'] = parsed_pgn.get("moves_count_per_player")
    return parsed_game

def extract_eco_codes(eco_codes_path: Path) -> pd.DataFrame:
    """Extracts data from the eco codes file
       run to test: extract_eco_codes('./data/eco_codes.csv')
    """
    df = pd.read_csv(eco_codes_path)
    return df

def _get_avg_move_time(valid_games:pd.DataFrame)-> pd.DataFrame:
      for index, row in valid_games.iterrows(): #iterate over dataframe
      # Use regex to find all timestamps
          timestamps = re.findall(r'\[%clk (\d+:\d+:\d+\.\d+)\]', row['pgn'])

      # Convert timestamps to seconds for easier calculations
          def convert_to_seconds(t):
              hours, minutes, seconds = t.split(':')
              return int(hours) * 3600 + int(minutes) * 60 + float(seconds)

      # Convert each timestamp to seconds
          times_in_seconds = [convert_to_seconds(time) for time in timestamps]

      # Calculate time spent on each move, separating by player
          white_times = times_in_seconds[0::2]
          black_times = times_in_seconds[1::2]

          def calculate_time_differences(times):
              return [times[i] - times[i + 1] for i in range(len(times) - 1)]

          white_time_spent = calculate_time_differences(white_times)
          black_time_spent = calculate_time_differences(black_times)

      # Compute average time spent per move for each player
          average_time_per_move_white = sum(white_time_spent) / len(white_time_spent) if white_time_spent else 0
          average_time_per_move_black = sum(black_time_spent) / len(black_time_spent) if black_time_spent else 0

          if row['user_color'] == "white":
              valid_games.at[index, 'user_avg_move_time_sec'] = average_time_per_move_white
          elif row['user_color'] == "black":
              valid_games.at[index, 'user_avg_move_time_sec'] = average_time_per_move_black
          else:
              raise Exception("The User does not have a valid color i.e either white or black")
      return valid_games

def transform(valid_games: pd.DataFrame, eco_codes: pd.DataFrame):
    valid_games=_get_avg_move_time(pd.DataFrame(valid_games))
    valid_games["start_date_time"]= valid_games["start_date"].astype(str) + " " + valid_games["start_time"].astype(str)
    valid_games['start_date_time'] = valid_games['start_date_time'].astype('datetime64')
    valid_games['end_date_time'] = pd.to_datetime(valid_games['end_date_time'], unit='s')
    valid_games.loc[valid_games['time_class'].isin(['bullet','blitz','rapid','daily'])]
    valid_games['rating_diff'] = valid_games['user_rating'] - valid_games['opponent_rating']
    valid_games['game_duration'] = valid_games['end_date_time'] - valid_games['start_date_time']
    valid_games['game_duration_sec'] = valid_games['game_duration'].dt.total_seconds()
    valid_games['game_duration_sec'] = valid_games['game_duration_sec'].astype('int')
    valid_games['game_duration'] = valid_games['game_duration'].apply(format_timedelta)
    valid_games['match_result'] = valid_games['pgn_result'].map({'1-0':'win','0-1':'defet','1/2-1/2':'draw'})
    valid_games['user_avg_move_time_sec'] =     valid_games['user_avg_move_time_sec'].round(1)
    transformed_games = valid_games.merge(eco_codes, on='ECO', how='left')
    transformed_games.drop(columns=['pgn','opponent_url','start_time','ECO','ECOUrl','pgn_result'], inplace=True)
    transformed_games.rename(columns={'time_class':'game_mode',
                                'moves_per_player':'rounds',
                                'result':'result_subcategory',
                                'Desc':'opening'}, inplace=True)
    transformed_games = transformed_games[['game_id',
                                           'game_url',
                                           'game_mode',
                                           'start_date',
                                           'username',
                                           'user_color',
                                           'user_rating',
                                           'user_accuracy',
                                           'opponent',
                                           'opponent_rating',
                                           'opponent_accuracy',
                                           'rating_diff',
                                           'match_result',
                                           'result_subcategory',
                                           'start_date_time',
                                           'end_date_time',
                                           'game_duration',
                                           'game_duration_sec',
                                           'rounds',
                                           'user_avg_move_time_sec',
                                           'opening']]
    return transformed_games

def transform_players(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the 'last_online' and 'joined' columns in players data to datetime format.
    
    Args:
        df (pd.DataFrame): The DataFrame containing the 'last_online' and 'joined' columns.
        
    Returns:
        pd.DataFrame: The DataFrame with transformed columns to datetime format.
    """

    df['last_online'] = pd.to_datetime(df['last_online'], unit='s')
    df['joined'] = pd.to_datetime(df['joined'], unit='s')

    return df

def format_timedelta(td):
    """
    Function to convert timedelta to HH:MM:ss
    """
    total_seconds = int(td.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f'{hours:02}:{minutes:02}:{seconds:02}'

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "overwrite",
) -> None:
    """
    Load dataframe to a database.

    Args:
        df: dataframe to load
        postgresql_client: postgresql client
        table: sqlalchemy table
        metadata: sqlalchemy metadata
        load_method: supports one of: [insert, upsert, overwrite]
    """
    if load_method == "insert":
        postgresql_client.insert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "upsert":
        postgresql_client.upsert(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    elif load_method == "overwrite":
        postgresql_client.overwrite(
            data=df.to_dict(orient="records"), table=table, metadata=metadata
        )
    else:
        raise Exception(
            "Please specify a correct load method: [insert, upsert, overwrite]"
        )
