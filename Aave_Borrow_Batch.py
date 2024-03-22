import requests
from datetime import datetime, timedelta

# GraphQL endpoint
url = "https://api.thegraph.com/subgraphs/name/messari/aave-v3-ethereum"

# Initial and end timestamps (UNIX timestamp format)
start_timestamp = 1704067200  # Adjust to your desired start timestamp
end_timestamp = 1706745600    # Adjust to your desired end timestamp

# Initialize variables for the loop
current_start_timestamp = start_timestamp
all_borrows = []

# Convert seconds to timedelta for 1 hour
time_step = timedelta(hours=1)

while current_start_timestamp < end_timestamp:
    # Calculate the end timestamp for the current 1-hour interval
    current_end_timestamp = int((datetime.utcfromtimestamp(current_start_timestamp) + time_step).timestamp())

    # Ensure not to exceed the overall end timestamp
    if current_end_timestamp > end_timestamp:
        current_end_timestamp = end_timestamp

    for inner_epoch in range(5):  # Loop through inner epochs to respect the pagination limit
        print(f"Fetching data for interval {datetime.utcfromtimestamp(current_start_timestamp).strftime('%Y-%m-%d %H:%M:%S')} to {datetime.utcfromtimestamp(current_end_timestamp).strftime('%Y-%m-%d %H:%M:%S')}")

        # GraphQL query for borrows within the current time interval and inner epoch
        query = """
        {
          borrows(first: 1000, skip: %s, where: {timestamp_gte: "%s", timestamp_lt: "%s"}, orderDirection: asc, subgraphError: allow) {
            amount
            amountUSD
            blockNumber
            gasLimit
            gasPrice
            gasUsed
            id
            logIndex
            hash
            nonce
            timestamp
            account {
              _eMode
              borrowCount
              closedPositionCount
              depositCount
              flashloanCount
              id
              liquidateCount
              liquidations(first: 10, orderBy: id, orderDirection: asc, skip: 10) {
                id
              } 
              liquidationCount
            }
          }
        }
        """ % (inner_epoch * 1000, current_start_timestamp, current_end_timestamp)

        headers = {"Content-Type": "application/json"}
        response = requests.post(url, json={"query": query}, headers=headers)

        if response.status_code == 200:
            data = response.json()
            borrows = data.get('data', {}).get('borrows', [])

            if not borrows:
                # No more borrows to fetch for this interval, break and move to the next
                break

            all_borrows.extend(borrows)

            # If less than 1000 borrows returned, likely fetched all for this interval
            if len(borrows) < 1000:
                break
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            break

    # Proceed to the next 1-hour interval
    current_start_timestamp = current_end_timestamp

print(f"Total borrows fetched: {len(all_borrows)}")
