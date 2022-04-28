import time
import requests
from flask import Flask, request
from flask_restful import Api
from twitter_connector import Twitter
from reddit_connector import Reddit
from kafka import KafkaProducer
import json
import argparse

app = Flask(__name__)
api = Api(app)

topic_list = ["cricket", "football", "sports"]
fetch_data_flag = True
twitter = Twitter()
reddit = Reddit()
broker = ""

prod = KafkaProducer(bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
                     value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                     api_version=(2, 5, 0)
                     )

def refresh_data(topics_to_loop):
    tweet_topic_map = {}
    reddit_topic_map = {}
    for topic in topics_to_loop:
        tweet_list = twitter.get_tweets_for_keyword(keyword=topic, lang='en')
        reddit_list = reddit.get_reddit_title_for_keyword(keyword=topic)
        tweet_topic_map[topic] = tweet_list
        reddit_topic_map[topic] = reddit_list
    return tweet_topic_map, reddit_topic_map

@app.route('/publish', methods=['POST'])
def publish_data():
    fetch_data_flag = True
    print("Started fetching Data")
    while fetch_data_flag:
        topics_to_loop = list(topic_list)
        tweet_topic_map, reddit_topic_map = refresh_data(topics_to_loop)
        for i in range(0, 100, 3):
            # tweet_list = twitter.get_tweets_for_keyword(keyword=topic, lang='en')
            # reddit_list = reddit.get_reddit_title_for_keyword(keyword=topic)
            for topic in topics_to_loop:
                tweet_list = tweet_topic_map[topic][i:i + 3]
                reddit_list = reddit_topic_map[topic][i: i + 3]
                data = {
                    "topic": topic,
                    "twitter_data_list": tweet_list,
                    "reddit_data_list": reddit_list
                }
                try:
                    print("Topic", topic, "Iteration", (i/3))
                    print('Fetched', len(tweet_list), 'values from index', i, '; Sending data to broker')
                    prod.send(topic, data)
                    prod.flush()
                except Exception as e:
                    print("GOT exception while trying to publish data")
                    print(e.with_traceback())
                    # raise e
            if len(topics_to_loop) < len(topic_list):
                # print("Found new topics to publish")
                # for new_topic in [new_topic for new_topic in topic_list if new_topic not in topics_to_loop]:
                #     print("Publishing new topic: ", new_topic)
                #     prod.send(new_topic, {
                #         "topic": new_topic,
                #         "twitter_data_list": ["Welcome to new TWITTER topic: " + str(new_topic),
                #                               "Data will be published in next iteration"],
                #         "reddit_data_list": ["Welcome to new REDDIT topic: " + str(new_topic),
                #                              "Data will be published in next iteration"]
                #     })
                break
            print("\nSleeping for 3 secs\n")
            time.sleep(3)

@app.route('/stop_publish', methods=['POST'])
def switch_data_flag():
    flag = False
    return {}, 200

@app.route('/advertise', methods=['POST'])
def create_publisher():
    for topic in request.json['topics']:
        if topic not in topic_list:
            topic_list.append(topic)
    return {"message": "Created PUBLISHERS and ADVERTISED all topics to Broker: " + 'broker', "code": 200}

@app.route('/create_publishers_for_topics', methods=['POST'])
def advertise():
    for topic in request.json['topics']:
        if topic not in topic_list:
            topic_list.append(topic)
    return {"message": "Created PUBLISHERS and ADVERTISED all topics to Broker: " + broker, "code": 200}


def create_publisher(new_topics):
    for topic in new_topics:
        if topic not in topic_list:
            topic_list.append(topic)
    print("Sending Advertise to BROKER: \"", broker, "\" with Topics:", new_topics)
    response = requests.post(url='http://' + broker + ':8990/advertise', json={"topics": new_topics})
    return {"message": "ADVERTISED the topics to Broker: " + broker, "code": 200}


if __name__ == '__main__':
    # topic_list = sys.argv[1:]
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--port", type=str, help="Port on which to start the app")
    parser.add_argument("--broker", type=str, help="Port on which to start the app")
    argv = parser.parse_args()

    broker = argv.broker
    app.run(host = '0.0.0.0', port = 8990)

# publish_data()
