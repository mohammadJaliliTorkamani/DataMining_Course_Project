import requests
import pandas as pd
import time
from tqdm import tqdm

API_KEY = "ec3bd0a8a9ba9bc7b056f812e9cc1710"

def get_credits(movie_id):
    url = f"https://api.themoviedb.org/3/movie/{movie_id}/credits"
    params = {"api_key": API_KEY}

    try:
        res = requests.get(url, params=params)
        data = res.json()

        # Top 5 actors
        cast = data.get("cast", [])
        actors = [c["name"] for c in cast[:5]]

        # Producers
        crew = data.get("crew", [])
        producers = [c["name"] for c in crew if c["job"] == "Producer"]

        # Director
        directors = [c["name"] for c in crew if c["job"] == "Director"]

        return {
            "actors": ", ".join(actors),
            "producers": ", ".join(producers),
            "director": ", ".join(directors)
        }

    except Exception:
        return {"actors": None, "producers": None, "director": None}


# Load dataset
df = pd.read_csv("./data/dataset2.csv")

# Create new columns
df["actors"] = None
df["producers"] = None
df["director"] = None

# Loop with progress bar
for i, row in enumerate(tqdm(df.itertuples(index=False), total=len(df))):
    credits = get_credits(row.id)

    df.at[i, "actors"] = credits["actors"]
    df.at[i, "producers"] = credits["producers"]
    df.at[i, "director"] = credits["director"]

    time.sleep(0.25)  # avoid rate limiting

# Save final dataset
df.to_csv("movies_with_cast.csv", index=False)