import twint
import pandas as pd

location = "46.769801,23.588877" # "46.784205, 23.678640"
radius = "4.5km" # "1.7km"

def scrape_cluj_tweets(search):
    """Scrape tweets based on search criteria."""

    c = twint.Config()
    c.Search = search
    c.Geo = f"{location},{radius}"
    c.Pandas = True
    c.Since = "2012-01-01"
    c.Until = "2022-01-01"

    twint.run.Search(c)

    tweet_data = twint.storage.panda.Tweets_df

    return tweet_data

def scrape_bulk_tweets(searchlist, hashtag=False):
    """Scrape tweets from a list of hashtags."""
    hashtag = '#' if hashtag else ''
    directory_name = 'hashtags' if hashtag else 'content'
    for search in searchlist:
        print(f"> Scraping tweets from {search}")
        data = scrape_cluj_tweets(f"{hashtag}{search}")
        data.to_csv(f"./data/{directory_name}/{search}.csv", index=False)
        print(f"> Scraped {len(data)} tweets from {search}")

def main():
    """Entry point."""
    
    # Scrape tweets from hashtags
    hashtags = [
        "cluj",
        "clujnapoca",
        "romania",
        "cluj-napoca",
        "untold",
        "electriccastle",
        "electric_castle",
        "electricastle",
        "festival",
        "tiff",
        "tiffcluj",
        "tiffclujnapoca",
        "tiffromania",
        "clujcareers",
        "careerscluj",
        "food",
        "streetfood",
        "foodstreet",
        "youth",
        "innovation",
        "future",
        "jobs",
        "job",
        "tech",
        "technology",
        "vot",
        "politics",
        "health",
        "sport",
        "sportcluj",
        "CFR",
        "CFRcluj",
        "UCluj",
        "fotbal",
        "football",
        "soccer",
        "volleyball",
        "volley",
        "tennis",
        "tenis",
        "basketball",
        "basket",
        "halep",
        "simonahalep",
        "simona",
        "handball",
        "cinemacity",
        "premiere",
        "premiera",
        "premieracluj",
        "music",
        "muzica",
        "concert",
        "teatru",
        "culture",
        "LGBT",
        "pride",
        "smartcity",
        "smart",
        "smartcitycluj",
        "climate",
        "animals",
        "fashion",
        "fashioncluj",
        "game",
        "muzica",
        "muzicacluj",
]

    contents = [
        "cluj",
        "cluj napoca",
        "romania",
        "cluj-napoca",
        "untold",
        "electric castle",
        "festival",
        "tiff",
        "tiff cluj",
        "career",
        "food",
        "stree food",
        "youth",
        "innovation",
        "future",
        "jobs",
        "job",
        "tech",
        "technology",
        "vot",
        "politics",
        "health",
        "sport",
        "sportcluj",
        "CFR",
        "CFR Cluj",
        "U Cluj",
        "fotbal",
        "soccer",
        "volleyball",
        "volley",
        "tennis",
        "tenis",
        "basketball",
        "basket",
        "halep",
        "simona halep",
        "simona",
        "handball",
        "cinema city",
        "premiere",
        "premiera",
        "premieracluj",
        "music",
        "muzica",
        "concert",
        "teatru",
        "culture",
        "LGBT",
        "pride",
        "smart city",
        "smart",
        "climate",
        "animals",
        "fashion",
        "fashion cluj",
        "game",
        "muzica",
        "muzicacluj",
]
    
    scrape_bulk_tweets(hashtags, hashtag=True)
    scrape_bulk_tweets(contents)

