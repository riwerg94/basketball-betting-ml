import time

from basketball_reference_web_scraper import client
from typing import AnyStr
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from autologging import traced, logged

@traced
@logged
class NBAParser:
    def __init__(self, year):
        self.__log.info("Parsing year: {}".format(year))
        self.year = year

    def parse_mvp_voting(self):
        url = 'https://www.basketball-reference.com/awards/awards_{}.html'.format(self.year)
        data = requests.get(url)

        self.save_file(data.text, path="data/raw_data/mvp_voting_html/{}.html".format(self.year))

    @staticmethod
    def save_file(data, path):
        with open(path, "w+", encoding="utf-8") as f:
            f.write(data)

    @staticmethod
    def clean_season_standings(standings):
        return pd.DataFrame(standings, index=[0])

    def parse_player_mvp_voting_html(self):
        with open(f"data/raw_data/mvp_voting_html/{self.year}.html", "r", encoding="utf-8") as f:
            page = f.read()

        mvp_table = self.drop_extra_headers(page_=page, name_="tr", class_="over_header", id_="mvp")

        mvps = pd.read_html(str(mvp_table))[0].assign(Year=self.year)

        return mvps

    def parse_player_per_game_html(self):
        with open(f"data/raw_data/player_per_game_html/{self.year}.html", "r", encoding="utf-8") as f:
            page = f.read()

        per_game_table = self.drop_extra_headers(page_=page, name_="tr", class_="thead", id_="per_game_stats")

        per_game = pd.read_html(str(per_game_table))[0].assign(Year=self.year)

        return per_game

    def parse_player_per_game_data(self):
        url = 'https://www.basketball-reference.com/leagues/NBA_{}_per_game.html'.format(self.year)
        driver = webdriver.Firefox(
            executable_path="C:/Users/rivverg/DontBeABitch/Projects/My Shit/basketball-betting-ml/src/dependencies/geckodriver.exe")

        driver.get(url)
        driver.execute_script("window.scrollTo(1,1000)")
        time.sleep(2)

        html = driver.page_source

        driver.close()

        self.save_file(html, path="data/raw_data/player_per_game_html/{}.html".format(self.year))

    def get_season_standings(self):
        standings = client.standings(season_end_year=self.year)
        return pd.concat(list((map(self.clean_season_standings, standings)))).assign(Year=self.year)

    @staticmethod
    def drop_extra_headers(page_, name_, class_, id_):
        soup = BeautifulSoup(page_, "html.parser")
        soup.find(name=name_, class_=class_).decompose()
        table = soup.find(id=id_)
        return table


class NBAPlayers:
    def __init__(self, year: int):
        self.advanced_stats = self.get_players_advanced_stats(year)
        self.player_identifiers = self.advanced_stats['slug']

    def get_players_advanced_stats(self, year, output_file_path=None):
        if output_file_path is not None:
            client.players_advanced_season_totals(year, output_file_path)
        else:
            return pd.DataFrame(client.players_advanced_season_totals(year))

    def get_players_basic_stats(self, year, output_file_path=None):
        if output_file_path is not None:
            client.players_season_totals(year, output_file_path)
        else:
            return pd.DataFrame(client.players_season_totals(year))

    @staticmethod
    def player_search(player_name: AnyStr):
        return client.search(player_name)

if __name__ == "__main__":
    NBAParser(year=2009).get_season_standings()