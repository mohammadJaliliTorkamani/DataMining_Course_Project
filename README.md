# CSCE-474/874 – Introduction to Data Mining
University: University of Nebraska–Lincoln (UNL)

Group Members: Mohammad Jalili Torkamani, Amirmohammad Sadeghnejad, Pedro Gomes, Jason Le

Final Project Dataset Enrichment Script

---

# TMDB Dataset Enrichment Tool

This Python script enriches a **TMDB movie dataset** with temporal features and movie credits information. It computes seasonal release data, days to the nearest Oscars, nearest U.S. holidays, and fetches top actors, producers, and directors from TMDB asynchronously.

---

## Features

- Adds **temporal features**:
  - Release season (`Winter`, `Spring`, `Summer`, `Fall`)
  - Days to nearest **Oscar ceremony**
  - Nearest **U.S. holiday** and days difference
- Fetches **movie credits** asynchronously:
  - Top 5 actors
  - Producers
  - Director(s)
- Outputs a new enriched CSV dataset.

---

## Requirements

- Python 3.9+
- TMDB API key
- Libraries:
  ```bash
  pip install aiohttp pandas tqdm python-dotenv holidays
  ```

- `.env` file containing your TMDB API key:
  ```text
  TMDB_API_KEY=your_api_key_here
  ```

---

## Usage

### Command-line

```bash
python enrich_tmdb_dataset.py <dataset_path> [--output <output_csv>]
```

- `<dataset_path>` – Path to your input CSV dataset.
- `--output` – Optional. Output CSV filename. Default: `movies_with_cast_temporal.csv`.

**Example:**

```bash
python enrich_tmdb_dataset.py data/movies.csv --output data/enriched_movies.csv
```

---

## Input CSV

- Must contain a column named `release_date`.
- Recommended additional column: `id` (TMDB movie ID).

**Example:**

| id       | title           | release_date |
|----------|----------------|--------------|
| 12345    | Example Movie   | 2022-03-15   |
| 67890    | Another Movie   | 2019-07-10   |

---

## Output CSV

The enriched dataset includes:

| id       | title           | release_date | release_season | days_to_oscar | nearest_holiday | days_to_nearest_holiday | actors              | producers           | director         |
|----------|----------------|--------------|----------------|---------------|----------------|-------------------------|-------------------|------------------|----------------|
| 12345    | Example Movie   | 2022-03-15   | Spring         | 12            | St. Patrick's Day | 3                       | Actor 1, Actor 2  | Producer 1       | Director 1     |

---

## How it Works

1. **Temporal Features**
   - Computes release season based on month.
   - Finds days to nearest Oscars (current and previous year).
   - Finds the closest U.S. holiday using the `holidays` library.

2. **Asynchronous TMDB Credits Fetch**
   - Uses `aiohttp` and `asyncio` to fetch credits concurrently.
   - Limits concurrency to 50 requests at a time to prevent rate-limiting.
   - Collects actors (top 5), producers, and directors for each movie.

3. **Dataset Enrichment**
   - Adds new columns to the DataFrame.
   - Saves enriched data to a CSV file.

---

## Notes

- Ensure your TMDB API key is valid and active.
- Handle large datasets with care; async requests can still take time for thousands of movies.
- Temporal features rely on accurate `release_date` parsing.

---

## License

MIT License

