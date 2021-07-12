from pymongo import MongoClient
from pymongo.errors import OperationFailure
import time
import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints.playergamelogs import PlayerGameLogs

# Connect to Mongo client
client = MongoClient()

# Use database
db = client['nbaDB']


# Create collections and save documents
def all_time_player_save():
    """
    Get basic info for all players for all time and save to collection.
    """
    try:
        all_players = players.get_players()
        ap = db.all_time_players
        ap.insert_many(all_players)
    except OperationFailure as e:
        print(e)


def season_game_log_save(year, collection):
    """
    Get single season game logs for all players and save to collection.
    """
    try:
        df = PlayerGameLogs(season_nullable=year).get_data_frames()[0]
        df_final = df.filter(
            ['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'GAME_ID', 'GAME_DATE', 'MATCHUP',
             'WL', 'MIN', 'FGM', 'FGA', 'FG_PCT', 'FG3M', 'FG3A', 'FG3_PCT', 'FTM', 'FTA', 'FT_PCT', 'OREB', 'DREB', 'REB',
             'AST', 'TOV', 'STL', 'BLK', 'BLKA', 'PF', 'PFD', 'PTS', 'PLUS_MINUS', 'NBA_FANTASY_PTS'], axis=1)
        collection.insert_many(df_final.to_dict('records'))
    except OperationFailure as e:
        print(e)


def all_seasons_save():
    """
    Get player game logs for every season from 1950-2021 and save to database.
    Each season is a new collection.
    """
    year_arr = [str(f'{i}-{str((i + 1))[-2:]}') for i in reversed(range(1950, 2021))]
    for year in year_arr:
        c_name = f'season_player_game_logs_{year.replace("-", "_")}'
        try:
            coll = db[c_name]
            season_game_log_save(year, coll)
            print(f'{c_name} successfully saved')
            time.sleep(1)
        except OperationFailure as e:
            print(f'{c_name} not saved!!')
            print(e)


# Save collections
# all_time_player_save()
all_seasons_save()
