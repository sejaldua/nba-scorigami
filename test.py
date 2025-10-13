from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv
import os

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

if __name__ == "__main__":
    post_tweet("Hello, world!")