from celery import Celery
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import datetime
import time
import os

# Configuration: Environment Variables for Better Security
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
REDIS_BROKER = os.getenv("REDIS_BROKER", "redis://localhost:6379/0")
BASE_URL = os.getenv("BASE_URL", "https://crex.live")

# MongoDB Configuration
client = MongoClient(MONGO_URI)
db = client["cricket_info"]
fixtures_collection = db["fixtures"]
match_details_collection = db["match_details"]
live_data_collection = db["live_data"]
scorecard_collection = db["scorecard"]

# Celery Configuration
app = Celery('tasks', broker=REDIS_BROKER, backend=REDIS_BROKER)

# Task to scrape fixtures
@app.task
def scrape_fixtures():
    url = f"{BASE_URL}/fixtures/match-list"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch fixtures: {e}"}

    soup = BeautifulSoup(response.text, 'html.parser')
    fixtures = []

    for match in soup.find_all("div", class_="match-card"):
        match_data = {
            "match_id": match.get("data-match-id", "unknown"),
            "title": match.find("div", class_="match-title").text.strip() if match.find("div", class_="match-title") else "N/A",
            "time": match.find("div", class_="match-time").text.strip() if match.find("div", class_="match-time") else "N/A",
            "status": "scheduled"
        }
        fixtures.append(match_data)
        # Upsert to MongoDB
        fixtures_collection.update_one({"match_id": match_data["match_id"]}, {"$set": match_data}, upsert=True)

    return fixtures

# Task to scrape match details
@app.task
def scrape_match_details(match_id):
    url = f"{BASE_URL}/match/{match_id}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch match details for match_id {match_id}: {e}"}

    soup = BeautifulSoup(response.text, 'html.parser')
    match_info = {}
    squads = {}

    # Parsing "Match info" section
    match_info_section = soup.find("div", class_="match-info")
    if match_info_section:
        match_info = {
            "venue": match_info_section.find("span", class_="venue").text.strip() if match_info_section.find("span", class_="venue") else "N/A",
            "umpires": match_info_section.find("span", class_="umpires").text.strip() if match_info_section.find("span", class_="umpires") else "N/A"
        }

    # Parsing "Squads" section
    squads_section = soup.find("div", class_="squads")
    if squads_section:
        squads = {
            "team_a": [player.text.strip() for player in squads_section.find_all("div", class_="team-a-player")],
            "team_b": [player.text.strip() for player in squads_section.find_all("div", class_="team-b-player")]
        }

    match_details_collection.update_one({"match_id": match_id}, {"$set": {"match_info": match_info, "squads": squads}}, upsert=True)
    return {"match_info": match_info, "squads": squads}

# Task to scrape live and scorecard data
@app.task
def scrape_live_and_scorecard(match_id):
    try:
        live_response = requests.get(f"{BASE_URL}/match/{match_id}/live", timeout=10)
        scorecard_response = requests.get(f"{BASE_URL}/match/{match_id}/scorecard", timeout=10)
        live_response.raise_for_status()
        scorecard_response.raise_for_status()
    except requests.RequestException as e:
        return {"error": f"Failed to fetch live/scorecard data for match_id {match_id}: {e}"}

    live_soup = BeautifulSoup(live_response.text, 'html.parser')
    scorecard_soup = BeautifulSoup(scorecard_response.text, 'html.parser')

    live_data = {}
    scorecard = {}

    # Parsing live data
    live_section = live_soup.find("div", class_="live-stats")
    if live_section:
        live_data = {
            "current_score": live_section.find("span", class_="score").text.strip() if live_section.find("span", class_="score") else "N/A",
            "current_over": live_section.find("span", class_="over").text.strip() if live_section.find("span", class_="over") else "N/A"
        }

    # Parsing scorecard data
    scorecard_section = scorecard_soup.find("div", class_="scorecard")
    if scorecard_section:
        scorecard = {
            "batsmen": [batsman.text.strip() for batsman in scorecard_section.find_all("div", class_="batsman")],
            "bowlers": [bowler.text.strip() for bowler in scorecard_section.find_all("div", class_="bowler")]
        }

    live_data_collection.update_one({"match_id": match_id}, {"$set": live_data}, upsert=True)
    scorecard_collection.update_one({"match_id": match_id}, {"$set": scorecard}, upsert=True)

    return {"live_data": live_data, "scorecard": scorecard}

# Periodic task to monitor match status
@app.task
def monitor_matches():
    now = datetime.datetime.now()
    for match in fixtures_collection.find({"status": "scheduled"}):
        match_time = datetime.datetime.strptime(match["time"], "%Y-%m-%d %H:%M:%S")
        if now >= match_time:
            fixtures_collection.update_one({"match_id": match["match_id"]}, {"$set": {"status": "live"}})
            scrape_live_and_scorecard.delay(match["match_id"])

# Main Scheduler
if __name__ == "__main__":
    while True:
        scrape_fixtures.delay()
        monitor_matches.delay()
        time.sleep(300)  # Run every 5 minutes
