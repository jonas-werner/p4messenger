###################################################################
#          ___
#         /   |
#  _ __  / /| |_ __ ___   ___  ___ ___  ___ _ __   __ _  ___ _ __
# | '_ \/ /_| | '_ ` _ \ / _ \/ __/ __|/ _ \ '_ \ / _` |/ _ \ '__|
# | |_) \___  | | | | | |  __/\__ \__ \  __/ | | | (_| |  __/ |
# | .__/    |_/_| |_| |_|\___||___/___/\___|_| |_|\__, |\___|_|
# | |                                              __/ |
# |_|                                             |___/
###################################################################
# Title:        p4messenger
# Version:      2.4
# Description:  Enters MQTT messages from EdgeX into InfluxDB
# Author:       Jonas Werner
###################################################################
import paho.mqtt.client as mqtt
import time
import json
import argparse
from influxdb import InfluxDBClient
from datetime import datetime

# Set environment variables
broker_address  = os.environ['p4messengerBrokerAddress']
topic           = os.environ['p4messengerTopic']
host            = os.environ['p4messengerHost']
port            = os.environ['p4messengerPort']
user            = os.environ['p4messengerUser']
password        = os.environ['p4messengerPass']
dbname          = os.environ['p4messengerDb']
p4mqttUser      = os.environ['p4mqttUser']
p4mqttPass      = os.environ['p4mqttPass']


def influxDBconnect():

    """Instantiate a connection to the InfluxDB."""
    influxDBConnection = InfluxDBClient(host, port, user, password, dbname)

    return influxDBConnection



def influxDBwrite(device, sensorName, sensorValue):

    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    measurementData = [
        {
            "measurement": device,
            "tags": {
                "gateway": device,
                "location": "Tokyo"
            },
            "time": timestamp,
            "fields": {
                sensorName: sensorValue
            }
        }
    ]
    influxDBConnection.write_points(measurementData, time_precision='ms')



def on_message(client, userdata, message):
    m = str(message.payload.decode("utf-8"))

    # Create a dictionary and extract the current values
    obj = json.loads(m)
    # current date and time
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    # Extract the data from each sensor, even if the MQTT message contain multiple entries
    for entry in obj["readings"]:
        print("Sensor: %s: Reading: %s" % (entry["name"], entry["value"]) )

        device      = entry["device"]
        sensorName  = entry["name"]
        sensorValue = entry["value"]

        # Write data to influxDB
        influxDBwrite(device, sensorName, sensorValue)




influxDBConnection = influxDBconnect()

print("Creating new instance ...")
client = mqtt.Client("sub1") #create new instance
client.on_message=on_message #attach function to callback
client.username_pw_set(p4mqttUser, p4mqttPass)

print("Connecting to broker ...")
client.connect(broker_address, 12967) #connect to broker
print("...done")

client.loop_start()



while True:
    client.subscribe(topic)
    time.sleep(1)

client.loop_stop()
