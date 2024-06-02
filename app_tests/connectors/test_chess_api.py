from dotenv import load_dotenv
from app.connectors.Chess import ChessApiClient
import os
import pytest


@pytest.fixture
def setup():
    load_dotenv(dotenv_path="app/.env")


def test_chess_client_get_players(setup):
    USER_AGENT = os.environ.get("USER_AGENT")
    chess_api_client = ChessApiClient('magnuscarlsen', user_agent=USER_AGENT)
    data = chess_api_client.get_user_info()

    assert type(data) == dict
    assert len(data) > 0
