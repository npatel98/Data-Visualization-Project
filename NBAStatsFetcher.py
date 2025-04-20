import requests
import pandas as pd
import time
import logging
from tqdm import tqdm

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

class StatsFetcher:
    def __init__(self, api_name, api_url, season, sort_by="PlayerName", ascending=True, delay=1, page_size=50):
        self.api_name = api_name
        self.api_url = api_url
        self.season = season
        self.sort_by = sort_by
        self.ascending = ascending
        self.delay = delay
        self.page_size = page_size
        self.teams = ['ATL', 'BOS', 'BRK', 'CHI', 'CHO', 'CLE', 'DAL', 'DEN', 'DET', 'GSW', 
                      'HOU', 'IND', 'LAC', 'LAL', 'MEM', 'MIA', 'MIL', 'MIN', 'NOP', 'NYK', 
                      'OKC', 'ORL', 'PHI', 'PHO', 'POR', 'SAC', 'SAS', 'TOR', 'UTA', 'WAS']
        self.data = []
        self.failed_requests = []

    def fetch_team_data(self, team):
        """Fetch paginated player data for a single team."""
        page = 1
        all_team_data = []

        while True:
            params = {
                "season": self.season,
                "team": team,
                "sortBy": self.sort_by,
                "ascending": self.ascending,
                "pageNumber": page,
                "pageSize": self.page_size
            }

            try:
                response = requests.get(self.api_url, params=params)
                response.raise_for_status()
                page_data = response.json()

                if not page_data:
                    break

                if isinstance(page_data, list):
                    all_team_data.extend(page_data)
                else:
                    all_team_data.append(page_data)

                if len(page_data) < self.page_size:
                    break

                page += 1
                time.sleep(self.delay)

            except requests.RequestException as e:
                error_message = str(e)
                # Log failed requests only
                logger.error(f"Failed to fetch data for {team}, page {page}: {error_message}")
                self.failed_requests.append({
                    "team": team,
                    "page": page,
                    "error": error_message
                })
                break

        return all_team_data

    def fetch_all(self):
        logger.info(f"Fetching NBA player stats from {self.api_name}...")
        for team in tqdm(self.teams, desc="Teams processed"):
            team_data = self.fetch_team_data(team)
            if not team_data:
                logger.warning(f"No data returned for team: {team}")
            self.data.extend(team_data)
            time.sleep(self.delay)

    def save_to_csv(self):
        """Save collected data to a CSV file."""
        if not self.data:
            logger.warning("No data to save.")
            return
        
        filename=f"{self.api_name}_{self.season}.csv"
        df = pd.DataFrame(self.data)
        df.to_csv(filename, index=False)
        logger.info(f"Data saved to {filename}")

    def get_dataframe(self):
        """Save collected data to a DataFrame."""
        if not self.data:
            self.fetch_all()
            self.log_failures_to_console()
            return pd.DataFrame(self.data)

        logger.info(f"Data fetched for {self.api_name}.")
        self.log_failures_to_console()
        return pd.DataFrame(self.data)

    def log_failures_to_console(self):
        """Log failed requests to the console."""
        if not self.failed_requests:
            logger.info("No failed requests.")
            return

        logger.error("\nFailed Requests Summary:")
        for fail in self.failed_requests:
            logger.error(f" - Team: {fail['team']}, Page: {fail['page']}, Error: {fail['error']}")