import pandas as pd
from autologging import traced, logged
from data import TEAM_ABBREVIATIONS_TO_TEAM
from basketball_reference_scraper.box_scores import get_box_scores
import basketball_reference_scraper.seasons as seasons
from parallel_webscrape import parallel_webscrape

#%%
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
        return series.str.upper().map(TEAM_ABBREVIATIONS_TO_TEAM)

#%%
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
@traced
@logged
def get_data():
    years = ['2022']
    for year in years:
        NBAData(season=year, to_csv=True).get_and_download_data()
