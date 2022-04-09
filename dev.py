#%%
import pandas as pd
from basketball_reference_scraper.box_scores import get_box_scores
import basketball_reference_scraper.seasons as seasons
import numpy as np
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
        self.nba_season.loc[:, "RESULT"] = self._get_results(self.nba_season)

    def get_nba_schedule(self):
        return seasons.get_schedule(self.season)

    @classmethod
    def _get_results(cls, season: pd.DataFrame):
        vect_func = np.vectorize(cls._label_winner)
        return vect_func(away_score=season['VISITOR_PTS'], home_score=season['HOME_PTS'])


class NBATeam(NBA):
    def __init__(self, team: str, season: str, add_features: bool = True):
        super().__init__(season)
        self.team = team
        self.utilities = NBA
        self.team_season = self.team_schedule()

        if add_features:
            (
                self._convert_name_to_abbreviation()
            )

    def team_schedule(self):
        nba_season = self.nba_season.copy()
        return nba_season[(nba_season.VISITOR.str.contains(pat=f"({self.team})", regex=True)) | (nba_season.HOME.str.contains(pat=f"({self.team})", regex=True))]

    def _convert_name_to_abbreviation(self):
        for i in ['HOME', 'VISITOR']:
            self.team_season[f"{i}_ABBR"] = self._name_to_abbreviation(self.team_season[i])

    def get_season_box_scores(self):

        self.team_season[['DATE', 'HOME_ABBR', 'VISITOR_ABBR']]





ex = parallel_scrape(list(df[['DATE', 'HOME_ABBR', 'VISITOR_ABBR']].to_records(index=False)))