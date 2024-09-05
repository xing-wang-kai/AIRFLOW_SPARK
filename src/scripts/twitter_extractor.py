from datetime import datetime, timedelta
import os
import requests
import json


FORMAT_DATE = "%Y-%m-%dT%H:%M:%S.00Z"
end_time= datetime.now().strftime(FORMAT_DATE)
util_time = (datetime.now() + timedelta(-1)).date().strftime(FORMAT_DATE)

query = "data engineer"

tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={util_time}&end_time={end_time}"

bearer_token = os.getenv("BEARER")

authorization={"Authorization": f"Bearer {bearer_token}"}

responses = requests.request("GET", url_raw, headers=authorization)

json_responses = responses.json()

print(json.dumps(json_responses, indent=16, sort_keys=True))

while 'next_token' in json_responses.get('meta', {}):
    next_token = json_responses['meta']['next_token']
    nxt_url = f"{url_raw}&next_token={next_token}"

    nxt_response = requests.request("GET", nxt_url, headers=authorization)

    nxt_json_responses = nxt_response.json()

    print(json.dumps(nxt_json_responses, indent=8, sort_keys=True))

