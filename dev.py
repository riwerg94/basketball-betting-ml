
import pandas as pd
import numpy as np
import basketball_reference_scraper.seasons as seasons

class NBA:
    def __init__(self, season):
        self.season = season

    @staticmethod
    def _label_winner(home_score: int, away_score: int):
        if home_score > away_score:
            return 'home'
        else:
            return 'away'

    def get_nba_schedule(self):
        return seasons.get_schedule(self.season)

    def _get_results(self, season: pd.DataFrame):
        vect_func = np.vectorize(self._label_winner)
        return vect_func(away_score=season['VISITOR_PTS'], home_score=season['HOME_PTS'])


class NBATeam(NBA):
    def __init__(self, team: str, season: str):
        super().__init__(season)
        self.team = team
        self.nba_season = self.get_nba_schedule()
        self.nba_season.loc[:, "RESULT"] = self._get_results(self.nba_season)

    def team_schedule(self):
        nba_season = self.nba_season.copy()
        return nba_season[(nba_season.VISITOR.str.contains(pat=f"({self.team})", regex=True)) | (nba_season.HOME.str.contains(pat=f"({self.team})", regex=True))]
