from flask import Flask, request, jsonify
from flask_restful import Api, Resource, reqparse
from collections import OrderedDict
import requests
import argparse

from flask import Flask, request, render_template
import requests
import argparse
import copy

app = Flask(__name__)
app.debug = True

global brokers_list, topics_list_global, brokers_to_topics_map
brokers_list = set()
topics_list_global = set()
brokers_to_topics_map = {}

@app.route('/register_broker', methods=['POST'])
def register_broker():
    broker_id = request.remote_addr
    brokers_list.add(broker_id)

    if 'topics' in request.json:
        topics = request.json['topics']
        topics_list_global.update(topics)
    # return_neighbours = []
    # for broker in brokers_list:
    #     return_neighbours.append(broker)
    #
    # return_neighbours.remove(broker_id)
    # return  {"brokers": return_neighbours, "code": 200}

    return {"Code": 200}

@app.route('/register_topic', methods=['POST'])
def register_topic():
    broker_id = request.remote_addr
    brokers_list.add(broker_id)

    topics = request.json['topics']
    topics_list_global.update(topics)

    return {"Code": 200}

@app.route('/deregister_topic', methods=['POST'])
def deregister_topic():

    topics = request.json['topics']
    for topic in topics:
        topics_list_global.remove(topic)

    return {"Code": 200}

@app.route('/register_broker_to_topic', methods=['POST'])
def register_broker_to_topic():

    broker_id = request.remote_addr
    topics_for_broker = set(request.json['topics'])
    topics_for_broker = list(topics_for_broker)
    topics_list_global.update(topics_for_broker)

    if broker_id not in brokers_to_topics_map:
        brokers_to_topics_map[broker_id] = list()

    topics_list_temp = brokers_to_topics_map.get(broker_id)
    topics_list_temp.extend(topics_for_broker)
    # for topic in topics_for_broker:
    #     if topic not in topics_list_temp:
    #         topics_list_temp.append(topic)

    return  {"code": 200}

@app.route('/get_all_topics', methods=['GET'])
def get_all_topics():
    return {"topics": list(topics_list_global), "code": 200}

@app.route('/get_neighbour_brokers', methods=['GET'])
def get_broker():
    broker_id = request.remote_addr

    return_neighbours = []
    for broker in brokers_list:
        return_neighbours.append(broker)

    return_neighbours.remove(broker_id)
    return  {"brokers": return_neighbours, "code": 200}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--port", type=str, help="Port on which to start the app")
    parser.add_argument("--brokers", nargs='*', type=str, help="List of neighbouring brokers")
    argv = parser.parse_args()


    app.run(host = '0.0.0.0', port = 8989)