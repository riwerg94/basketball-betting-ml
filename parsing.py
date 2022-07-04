import pandas as pd
from timeout_decorator import timeout
from basketball_reference_scraper.box_scores import get_box_scores

class NBAParser:
    def __init__(self):
        pass

    # @classmethod
    # def format_box_score(cls, teams: dict):

# @timeout(2.5)
def request_box_scores(date, home, visitor):
    pars = NBAParser()



    try:
        stats = get_box_scores(date, home, visitor)
    except:
         return

    home_team = stats[home].assign(GAME_DATE=pd.to_datetime(date)).assign(TEAM=home)
    visitor_team = stats[visitor].assign(GAME_DATE=pd.to_datetime(date)).assign(TEAM=visitor)

    box_score = pd.concat([home_team, visitor_team])

    return box_score