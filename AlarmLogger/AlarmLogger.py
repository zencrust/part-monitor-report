import paho.mqtt.client as mqtt
import collections
import logging
import time
import math
import os
import datetime
import pytz
import h5py

def init_hdf5(file_path):
    f = h5py.File(file_path, 'a')
    
    dset = f.create_dataset("data", (10, 10),dtype='f', maxshape=(None, 10), compression="gzip", compression_opts=9)

def main(mqtt_server: str):
    # The callback for when the client receives
    #  a CONNACK response from the server.
    def on_connect(client, userdata, flags, rc):
        logging.info("Connected with result code " + str(rc))

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        client.subscribe("partmonitor/#")

    # The callback for when a PUBLISH message is received from the server.
    def on_message(client, userdata, msg):
        logging.info(msg.topic + " " + str(msg.payload))
        [app_name, device_id, func, channel_name]  = str(msg.topic).rsplit('/', 4)[0:4]
        india_tz = pytz.timezone('Asia/Kolkata')
        msg_time = datetime.datetime.now(tz=india_tz)


    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(mqtt_server, 1883, 60)
    client.loop_forever()