import pandas as pd
import requests

class Reddit:
    
    def __init__(self):
        self.headers = None
        
    def token_create(self):

        # note that CLIENT_ID refers to 'personal use script' and SECRET_TOKEN to 'token'
        auth = requests.auth.HTTPBasicAuth('OcONgb5RwpCwvsVbcvfSCg', '2eQWQbGk0p7JmUvzBHm3_nF7MKfYCQ')

        # here we pass our login method (password), username, and password
        data = {'grant_type': 'password',
                'username': 'Fun_Appearance_5498',
                'password': 'facetwit@123'}

        # setup our header info, which gives reddit a brief description of our app
        headers = {'User-Agent': 'MyBot/0.0.1'}

        # send our request for an OAuth token
        res = requests.post('https://www.reddit.com/api/v1/access_token',
                            auth=auth, data=data, headers=headers)

        # convert response to JSON and pull access_token value
        TOKEN = res.json()['access_token']

        # add authorization to our headers dictionary
        self.headers = {**headers, **{'Authorization': f"bearer {TOKEN}"}}


    def get_reddit_title_for_keyword(self, keyword):
        
        self.token_create()
        res = requests.get("https://oauth.reddit.com/r/"+keyword+"/new?limit=100",
                   headers=self.headers)

        df = pd.DataFrame()  # initialize dataframe

        # loop through each post retrieved from GET request
        for post in res.json()['data']['children']:
            # append relevant data to dataframe
            df = df.append({
                'subreddit': post['data']['subreddit'],
                'title': post['data']['title'],
                'selftext': post['data']['selftext'],
                'upvote_ratio': post['data']['upvote_ratio'],
                'ups': post['data']['ups'],
                'downs': post['data']['downs'],
                'score': post['data']['score']
            }, ignore_index=True)

        print("Fetched {count} reddit posts".format(count=len(df['title'])))
        return list(df['title'])