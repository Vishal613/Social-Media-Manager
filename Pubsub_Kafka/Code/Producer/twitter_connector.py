import tweepy

class Twitter:
    def __init__(self):
        # self.auth1 = tweepy.OAuthHandler("9CIRioClI3LE9TqyzYFnpF2Y5", "GwxZieFYqK7bxUcbAQLYCqfvLvhY4T5dNakKo1m2PwXzreMN6P")
        # self.auth1.set_access_token("743685942515404805-9mwQVL9FKNG3Y6FZGaaQPjuTLQdzpV9", "NjiE4BpjfwylVRDEIzPJoxftla7m0EjScJ0izEdFVjkf8")
        # self.api1 = tweepy.API(self.auth1, wait_on_rate_limit=True)

        self.auth1 = tweepy.OAuthHandler('7DPvBpsdblfGIcbkktlj3uFAv',
                                         '4T4PDx6Uj4K9pgfG2JfCfz5hFyARd7L3yAOFa06vYj8yJvPq1a')
        self.auth1.set_access_token("1200030878753099776-e5pXYgEF8reOJffZKezJPyTtwUmUJt",
                                    "FRsuqbrks4ksTNOhCrtvzTL4Wm1CimUsNnmL7A10W2har")
        self.api1 = tweepy.API(self.auth1, wait_on_rate_limit=True)

        self.auth2 = tweepy.OAuthHandler('7DPvBpsdblfGIcbkktlj3uFAv', '4T4PDx6Uj4K9pgfG2JfCfz5hFyARd7L3yAOFa06vYj8yJvPq1a')
        self.auth2.set_access_token("1200030878753099776-e5pXYgEF8reOJffZKezJPyTtwUmUJt", "FRsuqbrks4ksTNOhCrtvzTL4Wm1CimUsNnmL7A10W2har")
        self.api2 = tweepy.API(self.auth2, wait_on_rate_limit=True)

        self.flag = True

    def get_tweets_offline(self, keyword):
        return self.solr_search_helper.search(keyword=keyword)

    def get_tweets_for_keyword(self, keyword, lang):
        print("Fetching tweets by Keyword: {keyword}".format(keyword=keyword))
        tweets_list = []

        query = keyword + ' ' + '-filter:retweets'
        if self.flag:
            twitter_api = self.api1
            self.flag = not self.flag
        else:
            twitter_api = self.api2
            self.flag = not self.flag

        for data in tweepy.Cursor(twitter_api.search_tweets, q=query, count=100, tweet_mode="extended", lang=lang, include_entities=True).pages(1):
            print("Fetched {count} tweets".format(count=100))
            for tweet in data:
                tweets_list.append(tweet.full_text)

        return tweets_list

    def get_solr_doc_from_tweet(self, tweet, keyword, lang):
        data_text = tweet.full_text
        user_name = tweet.author.screen_name

        doc = {
            'user_name': user_name,
            'data_text': data_text,
            'keyword': keyword,
            'language': lang
        }

        return doc

# if __name__ == "__main__":
#     twitter = Twitter()
#     twitter.get_tweets_for_keyword('covid')


