import pandas as pd
from basketball_reference_scraper.box_scores import get_box_scores
import basketball_reference_scraper.seasons as seasons
import numpy as np
from data import TEAM_TO_TEAM_ABBREVIATION
import data
from parallel_webscrape import parallel_webscrape
from autologging import traced, logged
import logging
import sys

@traced
@logged
class NBAUtility:
    @staticmethod
    def _label_winner(home_score: int, away_score: int):
        if home_score > away_score:
            return 'home'
        else:
            return 'away'

    @staticmethod
    def _name_to_abbreviation(series: pd.Series):
        return series.str.upper().map(TEAM_TO_TEAM_ABBREVIATION)

@traced
@logged
class NBAData(NBAUtility):
    def __init__(self, season, to_csv=True):
        self.nba = NBA(season=season)
        self.utility = NBAUtility
        self.season = season

    def get_and_download_data(self):

        schedule = self.get_team_schedules(self.season)
        schedule.to_csv(f".data/raw_data/nba_schedules/{self.season}_schedule_matches.csv")
        box_scores = self.get_box_scores(schedule)
        box_scores.to_csv(f".data/raw_data/box_scores/{self.season}_box_scores.csv")

    def _convert_name_to_abbreviation(self, df):
        self.__log.info("Converting names to abbreviated versions")
        for i in ['HOME', 'VISITOR']:
            df.loc[:, f"{i}_ABBR"] = self.nba._name_to_abbreviation(df[i])
        return df

    def get_box_scores(self, schedule):
        schedule = self._convert_name_to_abbreviation(schedule)
        df = pd.concat(parallel_webscrape(schedule[['DATE', 'HOME_ABBR', 'VISITOR_ABBR']].to_records(index=False)))
        self.__log.info(f"Finished getting box scores for {self.season}")
        return df

    def get_team_schedules(self, season):
        self.__log.info("Getting team schedules")
        df = seasons.get_schedule(season)
        return df[(~df.VISITOR_PTS.isna()) | (~df.HOME_PTS.isna())]


#%%
class NBAFE(NBAUtility):
    @staticmethod
    def create_10_game_history(df):
        for i in range(10):
            df = pd.concat([df, df.loc[: ,['DATE', 'VISITOR_PTS', 'HOME_PTS']].rename(columns={'DATE':f'historical_game_date_{i+1}', 'VISITOR_PTS':f'historical_game_away_{i+1}', 'HOME_PTS':f'historical_game_home_{i+1}'}).shift(i+1)], axis=1).fillna(0)
        return df

    @staticmethod
    def convert_dtypes(df):
        column_dtypes = {
            "HOME_PTS": "int64",
            "VISITOR_PTS": "int64"
        }
        return df.astype(dtype=column_dtypes)


#%%
class NBA(NBAFE):
    def __init__(self, season):
        self.season = season
        self.nba_season = self.get_nba_schedule()
        self.nba_season.loc[:, "RESULT"] = self._get_winner(self.nba_season)

    def get_nba_schedule(self):
        return seasons.get_schedule(self.season)

    @classmethod
    def _get_winner(cls, season: pd.DataFrame):
        vect_func = np.vectorize(cls._label_winner)
        return vect_func(away_score=season['VISITOR_PTS'], home_score=season['HOME_PTS'])


class NBATeam(NBA):
    def __init__(self, team: str, season: str, add_features: bool = True):
        super().__init__(season)
        self.team = team
        self.utilities = NBAUtility()
        self.team_season = self.team_schedule()
        self.season_dates = self.team_season['GAME_DATE'].values.tolist()


        if add_features:
            (
                self._convert_name_to_abbreviation()
            )
            season_box_scores = self.get_season_box_scores()

    def team_schedule(self):
        nba_season = self.nba_season.copy()
        return nba_season[(nba_season.VISITOR.str.contains(pat=f"({self.team})", regex=True)) | (nba_season.HOME.str.contains(pat=f"({self.team})", regex=True))]

    def _convert_name_to_abbreviation(self):
        for i in ['HOME', 'VISITOR']:
            self.team_season.loc[:, f"{i}_ABBR"] = self._name_to_abbreviation(self.team_season[i])

    def get_season_box_scores(self):
        df = pd.concat(parallel_webscrape(self.team_season[['DATE', 'HOME_ABBR', 'VISITOR_ABBR']].to_records(index=False)))

        df = df[~df.MP.str.contains("[a-zA-Z]")]

        for column in df.columns[~df.columns.isin(['GAME_DATE'])]:
            df[column] = pd.to_numeric(df[column], errors='ignore')

        return df

@traced
@logged
def get_data():
    years = ['2010']
    for year in years:
        NBAData(season=year, to_csv=True).get_and_download_data()


if __name__ == "__main__":
    logging.basicConfig(level = 20, stream = sys.stderr, format = "%(levelname)s:%(filename)s,%(lineno)d:%(name)s.%(funcName)s:%(message)s")
    get_data()