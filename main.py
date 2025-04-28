import feedparser

rss_feeds = {
    "BigQuery": "https://cloud.google.com/feeds/bigquery-release-notes.xml",
    "Cloud Run": "https://cloud.google.com/feeds/run-release-notes.xml",
    "Vertex AI": "https://cloud.google.com/feeds/vertex-ai-release-notes.xml",
    "GCP Blog": "https://cloud.google.com/blog/rss/"
}

for name, url in rss_feeds.items():
    print(f"--- {name} Updates ---")
    feed = feedparser.parse(url)
    for entry in feed.entries[:3]:  # last 3 entries
        print(f"{entry.title} ({entry.published})")
        print(f"{entry.link}\n")
