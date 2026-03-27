import pandas as pd
from datetime import datetime

# ---- Oscars actual dates ----
oscars_dates = {
    1929: datetime(1929, 5, 16), 1930: datetime(1930, 4, 3), 1931: datetime(1930, 11, 5),
    1932: datetime(1931, 11, 10), 1933: datetime(1932, 11, 18), 1934: datetime(1933, 3, 16),
    1935: datetime(1934, 2, 27), 1936: datetime(1935, 3, 5), 1937: datetime(1936, 3, 4),
    1938: datetime(1938, 3, 10), 1939: datetime(1939, 2, 23), 1940: datetime(1940, 2, 29),
    1941: datetime(1941, 2, 27), 1942: datetime(1942, 2, 26), 1943: datetime(1943, 3, 4),
    1944: datetime(1944, 3, 2), 1945: datetime(1945, 3, 15), 1946: datetime(1946, 3, 7),
    1947: datetime(1947, 3, 13), 1948: datetime(1948, 3, 20), 1949: datetime(1949, 3, 24),
    1950: datetime(1950, 3, 23), 1951: datetime(1951, 3, 29), 1952: datetime(1952, 3, 20),
    1953: datetime(1953, 3, 19), 1954: datetime(1954, 3, 25), 1955: datetime(1955, 3, 30),
    1956: datetime(1956, 3, 21), 1957: datetime(1957, 3, 27), 1958: datetime(1958, 3, 26),
    1959: datetime(1959, 4, 6), 1960: datetime(1960, 4, 4), 1961: datetime(1961, 4, 17),
    1962: datetime(1962, 4, 9), 1963: datetime(1963, 4, 8), 1964: datetime(1964, 4, 13),
    1965: datetime(1965, 4, 5), 1966: datetime(1966, 4, 18), 1967: datetime(1967, 4, 10),
    1968: datetime(1968, 4, 10), 1969: datetime(1969, 4, 7), 1970: datetime(1970, 4, 7),
    1971: datetime(1971, 4, 15), 1972: datetime(1972, 4, 10), 1973: datetime(1973, 3, 27),
    1974: datetime(1974, 4, 2), 1975: datetime(1975, 4, 8), 1976: datetime(1976, 3, 29),
    1977: datetime(1977, 3, 28), 1978: datetime(1978, 4, 3), 1979: datetime(1979, 4, 9),
    1980: datetime(1980, 4, 14), 1981: datetime(1981, 3, 31), 1982: datetime(1982, 3, 29),
    1983: datetime(1983, 4, 11), 1984: datetime(1984, 4, 9), 1985: datetime(1985, 3, 25),
    1986: datetime(1986, 3, 24), 1987: datetime(1987, 3, 30), 1988: datetime(1988, 4, 11),
    1989: datetime(1989, 3, 29), 1990: datetime(1990, 3, 26), 1991: datetime(1991, 3, 25),
    1992: datetime(1992, 3, 30), 1993: datetime(1993, 3, 29), 1994: datetime(1994, 3, 21),
    1995: datetime(1995, 3, 27), 1996: datetime(1996, 3, 25), 1997: datetime(1997, 3, 24),
    1998: datetime(1998, 3, 23), 1999: datetime(1999, 3, 21), 2000: datetime(2000, 3, 26),
    2001: datetime(2001, 3, 25), 2002: datetime(2002, 3, 24), 2003: datetime(2003, 3, 23),
    2004: datetime(2004, 2, 29), 2005: datetime(2005, 2, 27), 2006: datetime(2006, 3, 5),
    2007: datetime(2007, 2, 25), 2008: datetime(2008, 2, 24), 2009: datetime(2009, 2, 22),
    2010: datetime(2010, 3, 7), 2011: datetime(2011, 2, 27), 2012: datetime(2012, 2, 26),
    2013: datetime(2013, 2, 24), 2014: datetime(2014, 3, 2), 2015: datetime(2015, 2, 22),
    2016: datetime(2016, 2, 28), 2017: datetime(2017, 2, 26), 2018: datetime(2018, 3, 4),
    2019: datetime(2019, 2, 24), 2020: datetime(2020, 2, 9), 2021: datetime(2021, 4, 25),
    2022: datetime(2022, 3, 27), 2023: datetime(2023, 3, 12), 2024: datetime(2024, 3, 10),
    2025: datetime(2025, 3, 2), 2026: datetime(2026, 3, 15),
}

# Load CSV
df = pd.read_csv("/Users/mjalilitorkamani2/Documents/Intro to Data Mining/Final_Project/DataMining/movies_with_temporal_rating.csv")

# Ensure release date is datetime
df["release date"] = pd.to_datetime(df["release_date"])

# Function to compute days to next Oscar
def days_to_next_oscar(release_date):
    future_oscars = [date for date in oscars_dates.values() if date > release_date]
    if not future_oscars:
        return None  # no future Oscars
    next_oscars = min(future_oscars)
    return (next_oscars - release_date).days

# Apply function
df["days_to_oscar"] = df["release date"].apply(days_to_next_oscar)

# Save updated CSV (only updating the column)
df.to_csv("movies_with_oscar_updated.csv", index=False)