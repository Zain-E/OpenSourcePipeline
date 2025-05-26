import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import praw
import dlt
import polars as pl
from constants import REDDIT_CLIENT_ID, REDDIT_SECRET

reddit = praw.Reddit(
    client_id=REDDIT_CLIENT_ID,
    client_secret=REDDIT_SECRET,
    user_agent="dlt-reddit-pipeline by /u/PuzzleheadedAge7992"
)

def fetch_subreddit_posts(subreddit: str, limit: int = 100) -> list[dict]:
    sub = reddit.subreddit(subreddit)
    posts = []

    for post in sub.hot(limit=limit):
        posts.append({
            "id": post.id,
            "title": post.title,
            "score": post.score,
            "author": str(post.author),
            "created_utc": post.created_utc,
            "url": post.url,
            "num_comments": post.num_comments,
            "subreddit": post.subreddit.display_name
        })

    return posts

def top_subreddits_posts(top_n: int = 20, post_limit: int = 50) -> pl.DataFrame:
    top_subs = reddit.subreddits.popular(limit=top_n)

    all_posts = []
    for sub in top_subs:
        print(f"Fetching: r/{sub.display_name}")
        posts = fetch_subreddit_posts(sub.display_name, limit=post_limit)
        all_posts.extend(posts)

    df = pl.DataFrame(all_posts)

    return df


def df_to_file_system(df:pl.DataFrame) -> str:
    """
    Landnerds pipeline to load from a df to s3 storage.

    Args:
        full_table_name (str): The (full) name of the BigQuery table to load
        df (pl.DataFrame): The Polars DataFrame to load
    Returns:
        statement indicating the table has been saved to the filesystem.
    
    """
    table_name = "posts"
    arrow_table = df.to_arrow()
    resource = dlt.resource(arrow_table, name=table_name)

    # Create a dlt pipeline object
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_reddit",
        destination="filesystem",
        dataset_name="reddit"
    )

    # Run the pipeline
    load_info = pipeline.run(resource, loader_file_format="parquet")

    # Pretty print load information
    print(f"dlt load data: {load_info}")
    print(f"dataset of shape: {df.shape} uploaded!")


if __name__ == "__main__":
    df = top_subreddits_posts(top_n=20, post_limit=1000)
    df_to_file_system(df)
