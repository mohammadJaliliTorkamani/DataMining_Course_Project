import asyncio
import aiohttp
import pandas as pd
from tqdm import tqdm
from datetime import datetime
import holidays
import os
from dotenv import load_dotenv

# ---- Load API key from .env ----
load_dotenv()
API_KEY = os.getenv("TMDB_API_KEY")
if not API_KEY:
    raise ValueError("TMDB_API_KEY not found in .env file!")

BASE_URL = "https://api.themoviedb.org/3/movie/{}/credits"
CONCURRENT_REQUESTS = 30  # adjust based on TMDB rate limits

# ---- Oscars actual dates (2000-2026 example) ----
oscars_dates = {
    # 1970s - Often held in April
    1970: datetime(1970, 4, 7), 1971: datetime(1971, 4, 15),
    1972: datetime(1972, 4, 10), 1973: datetime(1973, 3, 27),
    1974: datetime(1974, 4, 2), 1975: datetime(1975, 4, 8),
    1976: datetime(1976, 3, 29), 1977: datetime(1977, 3, 28),
    1978: datetime(1978, 4, 3), 1979: datetime(1979, 4, 9),

    # 1980s
    1980: datetime(1980, 4, 14), 1981: datetime(1981, 3, 31),  # Delayed 1 day due to Reagan assassination attempt
    1982: datetime(1982, 3, 29), 1983: datetime(1983, 4, 11),
    1984: datetime(1984, 4, 9), 1985: datetime(1985, 3, 25),
    1986: datetime(1986, 3, 24), 1987: datetime(1987, 3, 30),
    1988: datetime(1988, 4, 11), 1989: datetime(1989, 3, 29),

    # 1990s
    1990: datetime(1990, 3, 26), 1991: datetime(1991, 3, 25),
    1992: datetime(1992, 3, 30), 1993: datetime(1993, 3, 29),
    1994: datetime(1994, 3, 21), 1995: datetime(1995, 3, 27),
    1996: datetime(1996, 3, 25), 1997: datetime(1997, 3, 24),
    1998: datetime(1998, 3, 23), 1999: datetime(1999, 3, 21),

    # 2000s - The shift to February begins in 2004
    2000: datetime(2000, 3, 26), 2001: datetime(2001, 3, 25),
    2002: datetime(2002, 3, 24), 2003: datetime(2003, 3, 23),
    2004: datetime(2004, 2, 29), 2005: datetime(2005, 2, 27),
    2006: datetime(2006, 3, 5), 2007: datetime(2007, 2, 25),
    2008: datetime(2008, 2, 24), 2009: datetime(2009, 2, 22),

    # 2010s
    2010: datetime(2010, 3, 7), 2011: datetime(2011, 2, 27),
    2012: datetime(2012, 2, 26), 2013: datetime(2013, 2, 24),
    2014: datetime(2014, 3, 2), 2015: datetime(2015, 2, 22),
    2016: datetime(2016, 2, 28), 2017: datetime(2017, 2, 26),
    2018: datetime(2018, 3, 4), 2019: datetime(2019, 2, 24),

    # 2020s
    2020: datetime(2020, 2, 9), 2021: datetime(2021, 4, 25),
    2022: datetime(2022, 3, 27), 2023: datetime(2023, 3, 12),
    2024: datetime(2024, 3, 10), 2025: datetime(2025, 3, 2),
    2026: datetime(2026, 3, 15),
}

# ---- US Holidays for all years in dataset ----
df_temp = pd.read_csv("./data/TMDB_dataset_raw2.csv", usecols=["release_date"])
df_temp['release_date'] = pd.to_datetime(df_temp['release_date'], errors='coerce')
years = range(df_temp['release_date'].dt.year.min(), df_temp['release_date'].dt.year.max()+1)
us_holidays = holidays.US(years=years)

# ---- Temporal feature functions ----
def get_season(month):
    if month in [12, 1, 2]:
        return 'Winter'
    elif month in [3, 4, 5]:
        return 'Spring'
    elif month in [6, 7, 8]:
        return 'Summer'
    else:
        return 'Fall'

def days_to_nearest_oscar(release_date):
    if pd.isna(release_date):
        return None
    year = release_date.year
    candidates = []
    if year in oscars_dates:
        candidates.append(pd.Timestamp(oscars_dates[year]))
    if (year - 1) in oscars_dates:
        candidates.append(pd.Timestamp(oscars_dates[year - 1]))
    if not candidates:
        return None
    return min(abs((release_date - d).days) for d in candidates)

def nearest_us_holiday_info(release_date):
    if pd.isna(release_date):
        return None, None
    release_date = pd.to_datetime(release_date)
    # convert holidays to Timestamp
    holiday_dates = [(pd.Timestamp(d), name) for d, name in us_holidays.items() if d.year == release_date.year]
    if not holiday_dates:
        return None, None
    nearest_date, nearest_name = min(
        holiday_dates,
        key=lambda x: abs((release_date - x[0]).days)
    )
    days_diff = abs((release_date - nearest_date).days)
    return nearest_name, days_diff

# ---- Async TMDB credit fetch ----
async def get_credits(session, movie_id, semaphore):
    url = BASE_URL.format(movie_id)
    params = {"api_key": API_KEY}

    async with semaphore:
        try:
            async with session.get(url, params=params) as res:
                data = await res.json()
                cast = data.get("cast", [])
                actors = [c["name"] for c in cast[:5]]
                crew = data.get("crew", [])
                producers = [c["name"] for c in crew if "Producer" in c["job"]]
                directors = [c["name"] for c in crew if c["job"] == "Director"]
                return {
                    "actors": ", ".join(actors),
                    "producers": ", ".join(producers),
                    "director": ", ".join(directors)
                }
        except:
            return {"actors": None, "producers": None, "director": None}

async def main(df):
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession() as session:
        tasks = [get_credits(session, row.id, semaphore) for row in df.itertuples(index=False)]
        results = []
        for f in tqdm(asyncio.as_completed(tasks), total=len(tasks)):
            res = await f
            results.append(res)
    return results

# ---- Load dataset ----
df = pd.read_csv("./data/TMDB_dataset_raw2.csv")
df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')

# ---- Compute temporal features ----
df['release_season'] = df['release_date'].dt.month.apply(get_season)
df['days_to_oscar'] = df['release_date'].apply(days_to_nearest_oscar)
df[['nearest_holiday', 'days_to_nearest_holiday']] = df['release_date'].apply(
    lambda x: pd.Series(nearest_us_holiday_info(x))
)

# ---- Fetch TMDB credits asynchronously ----
results = asyncio.run(main(df))
df['actors'] = [r["actors"] for r in results]
df['producers'] = [r["producers"] for r in results]
df['director'] = [r["director"] for r in results]

# ---- Save enriched dataset ----
df.to_csv("movies_with_cast_temporal.csv", index=False)