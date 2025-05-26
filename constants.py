import os
from dotenv import load_dotenv

load_dotenv()

# Env variables
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT")
BIGQUERY_SERVICE_ACCOUNT = os.getenv("BIGQUERY_PROJECT_ACCOUNT")
BIGQUERY_SERVICE_ACCOUNT_FILE = os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE")
BIGQUERY_SERVICE_ACCOUNT_FILE_ZAIN = os.getenv("BIGQUERY_SERVICE_ACCOUNT_FILE_ZAIN")
TYPE = os.getenv("TYPE")
PROJECT_ID = os.getenv("PROJECT_ID")
PRIVATE_KEY_ID = os.getenv("PRIVATE_KEY_ID")
PRIVATE_KEY = os.getenv("PRIVATE_KEY")
CLIENT_EMAIL = os.getenv("CLIENT_EMAIL")
CLIENT_ID = os.getenv("CLIENT_ID")
AUTH_URI = os.getenv("AUTH_URI")
TOKEN_URI = os.getenv("TOKEN_URI")
AUTH_PROVIDER_X509_CERT_URL = os.getenv("AUTH_PROVIDER_X509_CERT_URL")
CLIENT_X509_CERT_URL = os.getenv("CLIENT_X509_CERT_URL")
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
S3_BUCKET_URL = os.getenv("S3_POKEAPI_BUCKET_URL")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
REDDIT_CLIENT_ID = os.getenv("REDDIT_CLIENT_ID")
REDDIT_SECRET = os.getenv("REDDIT_SECRET")

# dbt constants
dbt_project_path = "/Users/zaineisa/Documents/VSCode/LandNerds_orchestration/LandNerds_datapipeline/landnerds"
dbt_profiles_dir = "/Users/zaineisa/Documents/VSCode/LandNerds_orchestration/LandNerds_datapipeline/landnerds/local_config"
dbt_sources = "/Users/zaineisa/Documents/VSCode/LandNerds_orchestration/LandNerds_datapipeline/landnerds/models/1_data_staging/to_warehouse"
