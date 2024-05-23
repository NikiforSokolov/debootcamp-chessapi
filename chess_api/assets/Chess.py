import sys
from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
from pathlib import Path
import pandas as pd

sys.path.insert(0, r"C:\Users\danielp\chess_git")
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

    dates = []
    current_date = start

    while current_date <= end:
        dates.append(current_date)
        current_date += relativedelta(months=1)

    # Ensure the last date is always the end_date
    if dates[-1] != end:
        dates[-1] = end

    return dates

def extract_games(start_date: str, end_date: str, username: str) -> pd.DataFrame:
    months = generate_monthly_dates(start_date, end_date)
    valid_games = []
    api = ChessApiClient(username)
    start_date = months[0]
    end_date = months[-1]

    for date in months:
        year = date.year
        month = date.month
        games = api.get_monthly_games(year=year, month=month)
        for game in games:
            parsed_game = parse_game(game, api.username)
            game_date = datetime.strptime(parsed_game.get("start_date"),'%Y-%m-%d')
            if start_date <= game_date <= end_date:
                valid_games.append(parsed_game)
        print(f"loaded games for the month of {year}-{month}")

    return pd.DataFrame(valid_games)

def extract_user_info(username: str) -> pd.DataFrame:
    api = ChessApiClient(username)
    df = pd.DataFrame(api.get_user_info())
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

