import pandas as pd
from data import Team, TEAM_ABBREVIATIONS_TO_TEAM, TEAM_TO_TEAM_ABBREVIATION
from multiprocessing import Pool
from pathlib import Path
import os


def encode_dnp_dnd(df):
    return df.assign(DNP_DND=lambda x: (x.MP=='Did Not Play') | (x.MP=='Did Not Dress')).astype({'DNP_DND': 'int64'})

def replace_dnp(df):
    return df.replace('Did Not Play', method='backfill')

def reindex_players(df, schedule):
    identity_col = ['PLAYER', 'TEAM']
    other_col = df.columns[~df.columns.isin(identity_col)]
    
    player = df.loc[:, 'PLAYER'].iloc[0]

    df = df.assign(GAME_DATE=lambda x: pd.to_datetime(x.index).date).set_index("GAME_DATE")

    df_other = df.loc[:, other_col]
    df_team_player = df.loc[:, identity_col].reindex(schedule, method='nearest')

    df = df_team_player.merge(df_other, left_index=True, right_index=True, how='left').fillna('Did Not Dress')

    return df

years = ['2010', '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018', '2019', '2020', '2021']

#
# for team in Team:
#     Path(f".data/not_raw_data/{team.value}/").mkdir(parents=True, exist_ok=True)
#
#     for year in years:
#         Path(f".data/not_raw_data/{team.value}/{year}/").mkdir(parents=True, exist_ok=True)
#
#         df_schedules = pd.read_csv(f".data/raw_data/nba_schedules/{year}_schedule_matches.csv").drop(columns=['Unnamed: 0'])
#
#         df_bs = pd.read_csv(f".data/raw_data/box_scores/{year}_box_scores.csv").drop(columns=['Unnamed: 0'])
#         df_bs = df_bs.loc[TEAM_ABBREVIATIONS_TO_TEAM[df_bs.TEAM] == team.value]
#
#         df_team = df_schedules[(df_schedules.HOME.str.upper() == team.value) | (df_schedules.VISITOR.str.upper() == team.value)]
#
#         df_team.loc[:, "HOME_ABBR"] = df_team.HOME.apply(lambda x: TEAM_TO_TEAM_ABBREVIATION[Team(x.upper())])
#         df_team.loc[:, "VISITOR_ABBR"] = df_team.VISITOR.apply(lambda x: TEAM_TO_TEAM_ABBREVIATION[Team(x.upper())])
#
#         df_bs_team = df_bs[df_bs.TEAM == TEAM_TO_TEAM_ABBREVIATION[team]].sort_values(['PLAYER', 'GAME_DATE'])#.set_index('GAME_DATE')
#
#         df_bs_team = df_bs_team[df_bs_team.PLAYER != 'Team Totals']
#         df_bs_team_totals = df_bs_team[df_bs_team.PLAYER == 'Team Totals']
#
#         team_schedule_ls = df_team.DATE.values.tolist()
#
#         df_bs_team_reidx = df_bs_team.groupby(['PLAYER','TEAM']).apply(reindex_players, team_schedule_ls).reset_index().set_index('GAME_DATE')
#         df_bs_team_reidx = df_bs_team_reidx.groupby(['PLAYER']).apply(encode_dnp_dnd)
#
#         df_bs_team_totals.to_csv(f".data/not_raw_data/{team.value}/{year}/{team.value.lower()}_{year}_team_totals.csv")
#         df_bs_team_reidx.to_csv(f".data/not_raw_data/{team.value}/{year}/{team.value.lower()}_{year}_box_score.csv")
#         df_team.to_csv(f".data/not_raw_data/{team.value}/{year}/{team.value.lower()}_{year}_schedule.csv")

def build_team_data(df_schedule, df_bs, team, year):
    df_team = df_schedule[(df_schedule.HOME.str.upper() == team.value) | (df_schedule.VISITOR.str.upper() == team.value)]

    # df_bs_players = df_bs[df_bs.PLAYER != 'Team Totals']

    df_team.loc[:, "HOME_ABBR"] = df_team.HOME.apply(lambda x: TEAM_TO_TEAM_ABBREVIATION[Team(x.upper())])
    df_team.loc[:, "VISITOR_ABBR"] = df_team.VISITOR.apply(lambda x: TEAM_TO_TEAM_ABBREVIATION[Team(x.upper())])

    team_schedule_ls = df_team.DATE.dt.date.values.tolist()

    df_bs_team = df_bs[df_bs.TEAM == TEAM_TO_TEAM_ABBREVIATION[team]].sort_values(['PLAYER', 'GAME_DATE']).set_index('GAME_DATE')

    df_bs_players = df_bs_team[df_bs_team.PLAYER != 'Team Totals']
    df_bs_team_totals = df_bs_team[df_bs_team.PLAYER == 'Team Totals']

    player_game_attendance = df_bs_team.groupby('PLAYER')['MP'].count() / len(team_schedule_ls)
    valid_player_ls = player_game_attendance[player_game_attendance > 0.5].index.values.tolist()

    df_bs_players = df_bs_players[df_bs_players.PLAYER.isin(valid_player_ls)]

    df_bs_players_reidx = df_bs_players.groupby(['PLAYER']).apply(reindex_players, team_schedule_ls).reset_index()#.set_index('GAME_DATE')
    df_bs_players_reidx_dn = df_bs_players_reidx.groupby(['PLAYER']).apply(encode_dnp_dnd)

    df_bs_team_totals.to_csv(f".data/not_raw_data/{team.value}/{year}/{'_'.join(team.value.lower().split(' '))}_{year}_team_totals.csv")
    df_bs_players_reidx.to_csv(f".data/not_raw_data/{team.value}/{year}/{'_'.join(team.value.lower().split(' '))}_{year}_box_score.csv")
    df_team.to_csv(f".data/not_raw_data/{team.value}/{year}/{'_'.join(team.value.lower().split(' '))}_{year}_schedule.csv")


data = pd.DataFrame()


for year in years:

    df_bs = pd.read_csv(f".data/raw_data/box_scores/{year}_box_scores.csv").drop(columns=['Unnamed: 0'])
    df_schedules = pd.read_csv(f".data/raw_data/nba_schedules/{year}_schedule_matches.csv").drop(columns=['Unnamed: 0'])

    df_schedules = df_schedules.assign(DATE=lambda x: pd.to_datetime(x.DATE))

    for team in Team:
        Path(f".data/not_raw_data/{team.value}/").mkdir(parents=True, exist_ok=True)
        Path(f".data/not_raw_data/{team.value}/{year}/").mkdir(parents=True, exist_ok=True)

        try:
            # data = pd.concat([data, build_team_data(df_schedules, team, year)])
            build_team_data(df_schedules, df_bs,  team, year)
        except:
            pass



numeric_columns = df_bs.columns[~df_bs.columns.isin(['PLAYER', 'GAME_DATE', 'TEAM', 'MP'])].values.tolist()

