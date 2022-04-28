from flask import Flask, request, render_template
import requests
import argparse
import copy

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

@app.route('/')
def my_form():
    global register_flag, messages
    print("HITTING WITHOUT")
    if (not register_flag):
        register_flag = True
        requests.post(url='http://' + broker + ':8990/register_subscriber')

    print('my_form')
    print(messages)
    if (socialmedia == {}):
        return render_template("my-form.html", list_to_send=data, notifications=messages,
                               unsubscribe=entered_undo_messages)
    else:
        return render_template("my-form.html", list_to_send=data, notifications=messages,
                               unsubscribe=new_undo_message)


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
            topics.append(key+'_'+socialmedia.get(key))
        #topics = {'topics': socialmedia}

        try:
            requests.post(url='http://' + broker + ':8990/subscriber/subscribe', params={'topics':topics})
        except Exception as e:
            data = {'Error': 'Central Broker Not Found'}
    entered_undo_messages.extend(undo_messages)
    messages.extend(undo_checkboxes)
    undo_messages = []
    # print('entered_undo_messages', entered_undo_messages)
    print('undo_checkboxes', undo_checkboxes)

    if (undo_checkboxes != []):
        undo_topics = {'topics': undo_checkboxes}

        try:
            for topic_unsubs in undo_topics['topics']:
                if topic_unsubs+'(twitter)' in data:
                    del data[topic_unsubs + '(twitter)']
                elif topic_unsubs+'(reddit)' in data:
                    del data[topic_unsubs + '(reddit)']
                elif topic_unsubs+'(both)' in data:
                    del data[topic_unsubs + '(both)']
                if topic_unsubs in data:
                    del data[topic_unsubs]
            requests.post(url='http://' + broker + ':8990/subscriber/unsubscribe', params=undo_topics)
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

                '''
                if topic in messages:
                    messages.remove(topic)
                if topic+' ( Twitter )' in messages:
                    messages.remove(topic+' ( Twitter )')
                if topic+' ( Reddit )' in messages:
                    messages.remove(topic+' ( Reddit )')

                if topic in entered_undo_messages:
                    entered_undo_messages.remove(topic)
                if topic+' ( Twitter )' in entered_undo_messages:
                    entered_undo_messages.remove(topic+' ( Twitter )')
                if topic+' ( Reddit )' in entered_undo_messages:
                    entered_undo_messages.remove(topic+' ( Reddit )')

                if topic in new_undo_message:
                    new_undo_message.remove(topic)
                if topic+' ( Twitter )' in new_undo_message:
                    new_undo_message.remove(topic+' ( Twitter )')
                if topic+' ( Reddit )' in new_undo_message:
                    new_undo_message.remove(topic+' ( Reddit )')
                '''

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
    app.run(host='0.0.0.0', port=argv.port)
    # app.run(host = '0.0.0.0', port = 1990)
