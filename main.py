from google.cloud import storage
import requests
import pandas as pd

aave_query = """
{
  deposits(where: {timestamp_gte: "1704067200", timestamp_lt: "1706745600"}) {
    id
    hash
    timestamp
    account {
      id
    }
    market {
      id
    }
    asset {
      id
      name
    }
    position {
      id
    }
    amount
    amountUSD
  }
}
"""

def api_to_gcs(url, filename, query):
    headers = {"Content-Type": "application/json"}
    response = requests.post(url, json={"query": query}, headers=headers)
    data = response.json()
    df = pd.json_normalize(data['data']['deposits'])
    client = storage.Client(project='keyboxweb')
    bucket = client.get_bucket('keybox-gcs-test')
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')

def main(data,context):
    api_to_gcs('https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum', 'test_aave_deposits.csv',aave_query)