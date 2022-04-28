from flask import Flask, request, render_template
import socket
import argparse
import copy
import json
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
import random
import time
from kafka.errors import NoBrokersAvailable

app = Flask(__name__)
app.debug = True
messages = []  # Topics
undo_messages = []  # Unsubscribed Topics
entered_undo_messages = []  # Topics with Socialmedia
new_undo_message = []
data = {}
socialmedia = {}
register_flag = False

broker = ""

topic_to_consumer_map = {}
seen_topics = []

@app.route('/')
def my_form():
    global register_flag, messages
    print("HITTING WITHOUT")
    kafka_avail_topics = []
    hostname = socket.gethostname()
    # broker_id = socket.gethostbyname(hostname)
    try:
        i =0
        consumer_topics = KafkaConsumer(bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'], enable_auto_commit='true',
                                        group_id=hostname, api_version=(2, 5, 0))
        kafka_avail_topics = consumer_topics.topics()
    except Exception as e:
        print("GOT exception while trying to fetch topics")
        print(e.with_traceback())
    except NoBrokersAvailable as e:
        print("GOT exception while trying to fetch topics")
        print(e.with_traceback())

    if (not register_flag) or (len(kafka_avail_topics) > len(seen_topics)):
        register_flag = True
        print("NEW topics found")
        messages.extend([t for t in kafka_avail_topics if t not in seen_topics])
        seen_topics.extend(kafka_avail_topics)

    print('my_form')
    print(new_undo_message)
    kafka_topics = []
    data_source_map = {}
    for msg in new_undo_message:
        topic_name = msg.split('(')[0].strip()
        data_source = msg.split('(')[1][:-1].strip().lower()
        data_source_map[topic_name] = data_source
        kafka_topics.append(topic_name)

    print('kafka_topic', kafka_topics)

    if (len(kafka_topics) > 0):
        events = []
        for topic in kafka_topics:
            print("Fetching data for topic:", topic)
            if topic not in topic_to_consumer_map:
                try:
                    consumer = KafkaConsumer(
                        bootstrap_servers=['kafka-1:9092', 'kafka-2:9092', 'kafka-3:9092'],
                        enable_auto_commit='true',
                        group_id=hostname + '_' + topic,
                        api_version = (2, 5, 0)
                    )
                    # + '_' + topic,
                    consumer.subscribe([topic])
                    topic_to_consumer_map[topic] = consumer
                except Exception as e:
                    print("GOT Exception")
                    print(e.with_traceback())
                except NoBrokersAvailable as e:
                    print("GOT Exception")
                    print(e.with_traceback())
            consumer = topic_to_consumer_map.get(topic)
            consumer.subscribe([topic])
            print('here')
            # read_lines = 0
            for msg in consumer:
                print(msg.offset)
                event_data = json.loads(msg.value)
                events.append(event_data)
                # read_lines += 1
                # if read_lines == len(kafka_topics):
                #     read_lines = 0
                #     break
                break
            consumer.commit()
        # consumer.close()

        # data['sports(twitter)'] = li
        for event_data in events:
            topic = event_data['topic']
            tweet_list = event_data['twitter_data_list']
            reddit_list = event_data['reddit_data_list']
            if topic in data_source_map:
                if data_source_map[topic].lower() == 'twitter' and (topic + ' ( Twitter )') in new_undo_message :
                    data[topic + ' (Twitter)'] = tweet_list
                elif data_source_map[topic].lower() == 'reddit' and (topic + ' ( Reddit )') in new_undo_message :
                    data[topic + ' (Reddit)'] = reddit_list
                elif data_source_map[topic].lower() == 'both' and (topic + ' ( Both )') in new_undo_message :
                    data[topic + ' (Both)'] = tweet_list + reddit_list

    print('data', data)
    if (socialmedia == {}):
        return render_template("my-form.html", list_to_send=data, notifications=messages, unsubscribe=entered_undo_messages)
    else:
        return render_template("my-form.html", list_to_send=data, notifications=messages, unsubscribe=new_undo_message)


@app.route('/', methods=['POST'])
def my_form_post():
    global messages, data, undo_messages, entered_undo_messages, socialmedia, new_undo_message
    print("HITTING POST")
    print('my_form_post', messages)
    checkboxes = []
    temp_messages = copy.deepcopy(messages)

    for checkbox in messages:
        # print('topic', checkbox)
        value = request.form.get(checkbox)
        # print(value)
        if value:
            checkboxes.append(checkbox)
            temp_messages.remove(checkbox)
            socialmedia[checkbox] = request.form['Social' + str(checkbox)]
            print(checkbox, socialmedia)

    undo_messages = copy.deepcopy(checkboxes)
    temp_entered_undo_messages = copy.deepcopy(entered_undo_messages)
    undo_checkboxes = []

    for checkbox in entered_undo_messages:
        print('topic', checkbox)
        value = request.form.get(checkbox)
        print(value)
        if value:
            undo_checkboxes.append(checkbox)
            temp_entered_undo_messages.remove(checkbox)

    messages = copy.deepcopy(temp_messages)
    entered_undo_messages = copy.deepcopy(temp_entered_undo_messages)
    print('checkboxes', checkboxes)

    if (checkboxes != []):

        topics = []
        for key in socialmedia:
            topics.append(key + '_' + socialmedia.get(key))
        # topics = {'topics': socialmedia}

    #         try:
    #             requests.post(url='http://' + broker + ':8990/subscriber/subscribe', params={'topics':topics})
    #             print('sent')
    #         except Exception as e:
    #             data = {'Error': 'Central Broker Not Found'}
    #             print('not sent')
    entered_undo_messages.extend(undo_messages)
    messages.extend(undo_checkboxes)
    undo_messages = []
    # print('entered_undo_messages', entered_undo_messages)
    print('undo_checkboxes', undo_checkboxes)

    if (undo_checkboxes != []):
        undo_topics = {'topics': undo_checkboxes}
        print('undo_topics', undo_topics)
        try:
            for topic_unsubs in undo_topics['topics']:
                if topic_unsubs + ' (Twitter)' in data:
                    del data[topic_unsubs + ' (Twitter)']
                elif topic_unsubs + ' (Reddit)' in data:
                    del data[topic_unsubs + ' (Reddit)']
                elif topic_unsubs + ' (Both)' in data:
                    del data[topic_unsubs + ' (Both)']
                if topic_unsubs in data:
                    del data[topic_unsubs]
        #             requests.post(url='http://' + broker + ':8990/subscriber/unsubscribe', params=undo_topics)
        except Exception as e:
            data = {'Error': 'Central Broker Not Found'}

    print('socialmedia', socialmedia)
    new_undo_message = []
    for msg in entered_undo_messages:
        new_undo_message.append(str(msg) + ' ( ' + str(socialmedia[msg]) + ' )')

    print('new_undo_message', new_undo_message)
    return render_template("my-form.html", list_to_send=data, notifications=messages,
                           unsubscribe=new_undo_message)


@app.route('/notify', methods=['POST'])
def my_form_notify():
    global messages
    global data

    print('Received Notify')

    advertise_flag = str(request.json['advertise_flag'])
    print(advertise_flag)
    if (advertise_flag == 'no'):
        temp_data = request.json['data']
        for key in temp_data.keys():
            display_key = key.split('_')[0] + '(' + key.split('_')[1].lower() + ')'
            data[display_key] = temp_data[key]
    else:
        advertise_action = request.json['advertise_action']
        Topics = request.json['topics']
        print("Topics:", Topics)
        if advertise_action.lower() == "advertise":
            if len(messages) == 0:
                messages = Topics
            else:
                for topic in Topics:
                    if topic not in messages:
                        messages.append(topic)
        else:
            for topic in Topics:

                remove_topic(topic, messages)
                remove_topic(topic, entered_undo_messages)
                remove_topic(topic, new_undo_message)

                if topic in data:
                    del data[topic]
                if topic + '(twitter)' in data:
                    del data[topic + '(twitter)']
                elif topic + '(reddit)' in data:
                    del data[topic + '(reddit)']
                elif topic + '(both)' in data:
                    del data[topic + '(both)']

    return "Received Notify"


def remove_topic(topic, input_list):
    if topic in input_list:
        input_list.remove(topic)
    if topic + ' ( Twitter )' in input_list:
        input_list.remove(topic + ' ( Twitter )')
    elif topic + ' ( Reddit )' in input_list:
        input_list.remove(topic + ' ( Reddit )')
    elif topic + ' ( Both )' in input_list:
        input_list.remove(topic + ' ( Both )')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--port", type=str, help="Port on which to start the app")
    parser.add_argument("--broker", type=str, help="Port on which to start the app")
    argv = parser.parse_args()

    broker = argv.broker
    #     app.run(host='0.0.0.0', port=argv.port)
    app.run(host='0.0.0.0', port=8991)
