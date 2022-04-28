import time
import requests
from flask import Flask, jsonify, request
from flask_restful import Api, Resource, reqparse
from twitter_connector import Twitter
from reddit_connector import Reddit
import argparse

app = Flask(__name__)
api = Api(app)

topic_list = list()
lang_list = ['en', 'es', 'hi']
fetch_data_flag = True
twitter = Twitter()
reddit = Reddit()
broker = ""


@app.route('/publish', methods=['POST'])
def publish_data():
    fetch_data_flag = True
    print("Started fetching Data")
    while fetch_data_flag:
        data_list = []
        for topic in topic_list:
            # for lang in lang_list:
            tweet_list = twitter.get_tweets_for_keyword(keyword=topic, lang='en')
            reddit_list = reddit.get_reddit_title_for_keyword(keyword=topic)
            print("Retrived tweets: ", len(tweet_list))
            data = {
                "topic": topic,
                "language": 'en',
                "twitter_data_list": tweet_list,
                "reddit_data_list": reddit_list
            }
            data_list.append(data)
        try:
            headers = {'Content-type': 'application/json'}
            print('Fetched Data. Sending data to broker')
            # response = requests.post(url='http://localhost:8990/receive_data', json=data_list)
            response = requests.post(url='http://' + broker + ':8990/receive_data', json=data_list)
        except Exception as e:
            print("GOT exception while trying to publish data")
            raise e
        time.sleep(300)

@app.route('/stop_publish', methods=['POST'])
def switch_data_flag():
    flag = False
    return {}, 200

@app.route('/advertise', methods=['POST'])
def advertise_to_broker():
    new_topics  = request.json['topics']
    return create_publisher(new_topics=new_topics)

@app.route('/deadvertise', methods=['POST'])
def deadvertise_to_broker():
    new_topics = request.json['topics']
    for topic in new_topics:
        if topic in topic_list:
            topic_list.remove(topic)
    # response = requests.post(url='http://localhost:8990/deadvertise', json={"topics": new_topics})
    response = requests.post(url='http://' + broker + ':8990/deadvertise', json={"topics": new_topics})
    return {"message": "Deadvertised the topics", "code": 200}

@app.route('/create_publishers_for_topics', methods=['POST'])
def create_publisher():
    for topic in request.json['topics']:
        if topic not in topic_list:
            topic_list.append(topic)
    # response = requests.post(url='http://localhost:8990/advertise', json={"topics": topic_list})
    response = requests.post(url='http://' + broker + ':8990/advertise', json={"topics": topic_list})
    return {"message": "Created PUBLISHERS and ADVERTISED all topics to Broker: "+ broker, "code": 200}

def create_publisher(new_topics):
    for topic in new_topics:
        if topic not in topic_list:
            topic_list.append(topic)
    print("Sending Advertise to BROKER: \"", broker, "\" with Topics:", new_topics)
    # response = requests.post(url='http://localhost:8990/advertise', json={"topics": new_topics})
    response = requests.post(url='http://' + broker + ':8990/advertise', json={"topics": new_topics})
    return {"message": "ADVERTISED the topics to Broker: "+broker, "code": 200}

if __name__ == '__main__':
    # topic_list = sys.argv[1:]
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--port", type=str, help="Port on which to start the app")
    parser.add_argument("--broker", type=str, help="Port on which to start the app")
    argv = parser.parse_args()

    broker = argv.broker
    app.run(host = '0.0.0.0', port = argv.port)

