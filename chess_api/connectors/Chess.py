import requests
from datetime import datetime
from typing import Union
from requests import JSONDecodeError
import re

class ChessApiClient:
    def __init__(self, username: str):
        """
        Class to connect to chess.com API

        Parameters:
        - username (str): a chess.com user's username

        """
        self.username = username
        self.api_path = "http://api.chess.com/pub"
        self.headers = {'User-Agent': 'username: danihell, email: danihello@gmail.com'}

    def get_archive_urls(self) -> list:
        """
        Returns a list of urls of months played by a user
        """
        response = requests.get(url=f"{self.api_path}/player/{self.username}/games/archives", headers=self.headers)
        if response.status_code == 200 and response.json().get("archives") is not None:
            return response.json().get("archives")
        else:
            raise JSONDecodeError(f"failed to extract data from chess API. status Codes: {response.status_code}. Response: {response.text}")

    def get_monthly_games(self, year: int, month: int) -> list:
        """
        Returns a list of games played on chess.com by a user in a month

        Parameters:
        - year (int): the year the games were played
        - month (int): the month the games were played
        """
        url = f"{self.api_path}/player/{self.username}/games/{year}/{str(month).zfill(2)}"
        response = requests.get(url=url, headers=self.headers)
        if response.status_code == 200 and response.json().get("games") is not None:
            return response.json().get("games")

    def get_user_info(self) -> dict:
        """
        Returns info about the user
        """
        response = requests.get(url=f"{self.api_path}/player/{self.username}", headers=self.headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"failed to extract data from chess API. status Codes: {response.status_code}. Response: {response.text}")

    def get_user_country(self) -> dict:
        """
        Returns info about the country origin of the user
        """
        info = self.get_user_info()
        country_url = info.get("country")
        if country_url is not None:
            response = requests.get(url=country_url, headers=self.headers)
            if response.status_code == 200:
                return response.json()
            else:
                raise Exception(f"failed to extract data from chess API. status Codes: {response.status_code}. Response: {response.text}")
        else:
            return None

if __name__ == "__main__":
    api = ChessApiClient("dolols")

