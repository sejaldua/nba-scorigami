import random
import time
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv
import os
import requests
import pandas as pd
from tqdm import tqdm
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


def get_all_scores_data(start=1996, end=2025):
    final_df = pd.DataFrame()
    for season in tqdm(range(start, end + 1)):
        try:
            df = get_season_data(season)
            final_df = pd.concat([final_df, df])
        except Exception as e:
            print(f"[warn] Skipping season {season}: {e}")

        # random delay to prevent triggering rate limit
        sleep_time = 3 + random.random() * 2
        time.sleep(sleep_time)

    score_freq = final_df.pivot_table(
        index='PTS_L',
        columns='PTS_W',
        aggfunc='size',
        fill_value=0
    )
    return score_freq, final_df

def check_scorigami(pts_w, pts_l):
    if pts_w <= pts_l:
        return "Invalid score combination: Winning points must be greater than losing points."
    freq = score_freq.at[pts_l, pts_w]
    if freq == 0:
        return f"Scorigami! The score combination {pts_w}-{pts_l} has never occurred."
    else:
        return f"The score combination {pts_w}-{pts_l} has occurred {freq} times. The last time it occurred was on {final_df[(final_df['PTS_W'] == pts_w) & (final_df['PTS_L'] == pts_l)]['GAME_DATE'].max()} when the {final_df[(final_df['PTS_W'] == pts_w) & (final_df['PTS_L'] == pts_l)]['TEAM_NAME_W'].values[0]} defeated the {final_df[(final_df['PTS_W'] == pts_w) & (final_df['PTS_L'] == pts_l)]['TEAM_NAME_L'].values[0]}."

todays_date = pd.Timestamp.now().strftime("%Y%m%d")
print(todays_date)
games_df = get_nba_games(todays_date)
if not games_df.empty:
    score_freq, final_df = get_all_scores_data()
    for idx, row in games_df.iterrows():
        # check if the game is final and the game has not been tweeted yet
        with open("tweeted_games.txt", "a+") as f:
            f.seek(0)
            tweeted_games = f.read().splitlines()
            game_identifier = f"{row['away_team']}@{row['home_team']} | {row['date']}"
            if row['status'] == "Final" and game_identifier not in tweeted_games:
                print("New final game found:", game_identifier)
                result = check_scorigami(max(row['home_score'], row['away_score']), min(row['home_score'], row['away_score']))
                tweet_text = (
                    f"{row['away_team']} @ {row['home_team']}\n"
                    f"Score: {row['away_score']} - {row['home_score']}\n\n"
                    f"{result}"
                )
                post_tweet(tweet_text)
                f.write(game_identifier + "\n")
            elif game_identifier in tweeted_games:
                print("Game already tweeted:", game_identifier)