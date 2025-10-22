import random
import time
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv
import os
import requests
import pandas as pd
from tqdm import tqdm
import urllib
from nba_api.stats.endpoints import leaguegamelog
from requests.exceptions import ReadTimeout, RequestException

load_dotenv()

api_key = os.getenv("TWITTER_API_KEY")
api_secret = os.getenv("TWITTER_API_SECRET")
access_token = os.getenv("TWITTER_ACCESS_TOKEN")
access_secret = os.getenv("TWITTER_ACCESS_SECRET")

def post_tweet(text: str):
    client = OAuth1Session(
        client_key=api_key,
        client_secret=api_secret,
        resource_owner_key=access_token,
        resource_owner_secret=access_secret,
        signature_method="HMAC-SHA1",
    )

    url = "https://api.twitter.com/2/tweets"
    payload = {"text": text}

    resp = client.post(url, json=payload)
    resp.raise_for_status()
    data = resp.json()
    print("Tweeted:", data)
    return data

def get_nba_games(date_str):
    url = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
    params = {"dates": date_str}  # format: YYYYMMDD
    r = requests.get(url, params=params)
    data = r.json()

    games = []
    for event in data.get("events", []):
        competition = event["competitions"][0]
        home = competition["competitors"][0]
        away = competition["competitors"][1]
        games.append({
            "date": event["date"],
            "home_team": home["team"]["displayName"],
            "away_team": away["team"]["displayName"],
            "home_score": int(home["score"]),
            "away_score": int(away["score"]),
            "status": competition["status"]["type"]["description"],
        })

    return pd.DataFrame(games)

def safe_leaguegamelog(season, max_retries=5, base_sleep=1.0):
    """
    Call LeagueGameLog with retries, exponential backoff, and jitter.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            # main call with increased timeout
            gamefinder = leaguegamelog.LeagueGameLog(
                league_id='00',
                season=season,
                timeout=(5, 120)  # (connect timeout, read timeout)
            )
            df = gamefinder.get_data_frames()[0]
            return df

        except ReadTimeout:
            attempt += 1
            wait = base_sleep * (2 ** (attempt - 1)) * (1 + random.random() * 0.3)
            print(f"[timeout] Season {season}: attempt {attempt}, sleeping {wait:.1f}s...")
            time.sleep(wait)

        except RequestException as e:
            attempt += 1
            wait = base_sleep * (2 ** (attempt - 1)) * (1 + random.random() * 0.3)
            print(f"[network error] {e} — sleeping {wait:.1f}s before retrying...")
            time.sleep(wait)

        except Exception as e:
            attempt += 1
            wait = base_sleep * (2 ** (attempt - 1))
            print(f"[error] {e} — sleeping {wait:.1f}s before retrying...")
            time.sleep(wait)

    raise RuntimeError(f"Failed to fetch season {season} after {max_retries} attempts.")


# -------------------------------
# Your original functions, adapted
# -------------------------------
def get_season_data(season):
    games = safe_leaguegamelog(season)

    # replicate your transformation logic
    games['IS_HOME'] = games['MATCHUP'].apply(lambda x: 0 if '@' in x else 1)
    winning_teams = games[games['WL'] == "W"][['SEASON_ID', 'GAME_ID', 'GAME_DATE', 'MATCHUP', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'PTS', 'IS_HOME']]
    losing_teams = games[games['WL'] == "L"][['GAME_ID', 'TEAM_ABBREVIATION', 'TEAM_NAME', 'PTS', 'IS_HOME']]
    merged = pd.merge(winning_teams, losing_teams, on='GAME_ID', suffixes=('_W', '_L'))
    merged['MARGIN'] = merged['IS_HOME_W'].apply(lambda x: 1 if x == 1 else -1) * (merged['PTS_W'] - merged['PTS_L'])
    return merged


def get_all_scores_data(season=2025):
    existing_df = pd.read_csv('/home/sdua/nba-scorigami/nba_game_scores_1946_2024.csv')
    new_df = get_season_data(season)

    # concatenate preloaded dataframe with new data from this season
    final_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=['GAME_ID']).reset_index(drop=True)

    score_freq = final_df.pivot_table(
        index='PTS_L',
        columns='PTS_W',
        aggfunc='size',
        fill_value=0
    )
    # final_df.to_csv('nba_game_scores_1946_2024.csv', index=False)
    return score_freq, final_df

def check_scorigami(pts_w, pts_l, current_date):
    if pts_w <= pts_l:
        return "Invalid score combination: Winning points must be greater than losing points."
    freq = score_freq.at[pts_l, pts_w]
    if freq == 0:
        return f"Scorigami! The score combination {pts_w}-{pts_l} has never occurred."
    else:
        # Filter out today's games when finding last occurrence
        historical_games = final_df[
            (final_df['PTS_W'] == pts_w) & 
            (final_df['PTS_L'] == pts_l) &
            (final_df['GAME_DATE'] != current_date)
        ]
        
        if historical_games.empty:
            # This score has only occurred today (first time)
            return f"Scorigami! The score combination {pts_w}-{pts_l} has never occurred before today."
        else:
            last_date = historical_games['GAME_DATE'].max()
            last_game = historical_games[historical_games['GAME_DATE'] == last_date].iloc[-1]
            return f"The score combination {pts_w}-{pts_l} has occurred {freq} times. The last time it occurred was on {last_date} when the {last_game['TEAM_NAME_W']} defeated the {last_game['TEAM_NAME_L']}."

todays_date = pd.Timestamp.now()
yesterdays_date = (pd.Timestamp.now() - pd.Timedelta(days=1))
dates_to_run = [todays_date, yesterdays_date]
for date in dates_to_run:
    date_simple = date.strftime("%Y%m%d")
    date_for_tweet = date.strftime("%B %d, %Y")
    games_df = get_nba_games(date_simple)
    if not games_df.empty:
        score_freq, final_df = get_all_scores_data()
        for idx, row in games_df.iterrows():
            # check if the game is final and the game has not been tweeted yet
            with open("/home/sdua/nba-scorigami/tweeted_games.txt", "a+") as f:
                f.seek(0)
                tweeted_games = f.read().splitlines()
                game_identifier = f"{row['away_team']}@{row['home_team']} | {row['date']}"
                if row['status'] == "Final" and game_identifier not in tweeted_games:
                    print("New final game found:", game_identifier)
                    # Convert today's date to GAME_DATE format (YYYY-MM-DD or similar)
                    current_game_date = pd.Timestamp(date_simple).strftime("%Y-%m-%d")
                    result = check_scorigami(
                        max(row['home_score'], row['away_score']), 
                        min(row['home_score'], row['away_score']),
                        current_game_date
                    )
                    tweet_text = (
                        f"{date_for_tweet}\n"
                        f"{row['away_team']} @ {row['home_team']}\n"
                        f"Score: {row['away_score']} - {row['home_score']}\n\n"
                        f"{result}"
                    )
                    post_tweet(tweet_text)
                    f.write(game_identifier + "\n")
                elif game_identifier in tweeted_games:
                    print("Game already tweeted:", game_identifier)
