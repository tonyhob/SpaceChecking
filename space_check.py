
import RPi.GPIO as GPIO
import time
from Bluetin_Echo import Echo
import paho.mqtt.client as mqtt
import mysql.connector
import json
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s',
                    filename='spacecheck.log',
                    filemode='w')

# addr = "CB201"
# pub_logevent_topic = "sps/log"

mqtt_server = "192.168.1.3"
mqtt_port = 1883
mqtt_alive = 60
sub_topic = "sps/CB201/downlink"
pub_topic = "sps/CB201"

dis_requirement = 20
samples = 5
speed_of_sound = 315

ent_trig_pin = 23
ent_echo_pin = 24
trig_pin = [7, 16, 25]
echo_pin = [1, 20, 8]
led = 21


GPIO.setmode(GPIO.BCM)
GPIO.setup(led, GPIO.OUT)
GPIO.setwarnings(False)

status = [0] * len(trig_pin)
prev_status = [0] * len(trig_pin)
ent_status = 0
echo = []

ent_echo = Echo(ent_trig_pin, ent_echo_pin, speed_of_sound)
for i in range(len(trig_pin)):
    echo.append(Echo(trig_pin[i], echo_pin[i], speed_of_sound))


def led_blink():
    for x in range(0,2):
        GPIO.output(led,True)
        time.sleep(0.3)
        GPIO.output(led,False)
        time.sleep(0.1)

def log_event(event,event_detail):
    client.publish(pub_topic, str(event) + ";" + event_detail)
    cur = mdb.cursor()
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    cur.execute("INSERT INTO system_log (Time ,Event , Details) VALUES (%s, %s, %s)", (dt, event, event_detail))
    mdb.commit()
    logging.info(str(event)+ ", " + event_detail)
    led_blink()

def on_connect(client, userdata, flags, rc):
    logging.debug("Connected mqtt with result code "+str(rc))
    client.subscribe(sub_topic)

def on_message(client, userdata, msg):
    logging.info(msg.topic+" "+str(msg.payload))


def get_ent_status(ent_status):
    ent_sendflag = False
    ent_prev_status = ent_status
    dis = ent_echo.read('cm', samples)
    if dis < dis_requirement:
        ent_status = 1
    else:
        ent_status = 0
    if ent_prev_status != ent_status:
        ent_sendflag = True
        logging.debug("ent_status="+str(ent_status))
    return ent_sendflag, ent_status


def get_all_avail():
    sendflag = False
    prev_status = status.copy()
    for i in range(len(trig_pin)):
        dis = echo[i].read('cm', samples)
        # logging.debug(str(dis), end=", ")
        logging.debug(str(dis))
        if dis > dis_requirement:
            status[i] = 0
        else:
            status[i] = 1
        if prev_status[i] != status[i]:
            sendflag = True
    prev_status.clear()
    return sendflag

def avail_to_db(status):
    cur = mdb.cursor()
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    sql = "INSERT INTO CB201 (Time, "
    for i in range(len(status)):
        sql = sql + "s" + str(i+1) +", "
    sql = sql.strip(', ') + ") VALUES (%s, " + ("%s, ")*(len(trig_pin)-1) + "%s)"
    temp_val = [dt]
    for i in status:
        temp_val.append(i)
    val=tuple(temp_val)
    cur.execute(sql, val)
    mdb.commit()

    mqttdata = {1000+i:status[i] for i in range(len(status))}
    log_event(221, json.dumps(mqttdata))
    logging.debug("sent to db: " + str(status))
    led_blink()


dbsetup = 0   
mqttsetup = 0
client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

try: 
    mdb = mysql.connector.connect(host="192.168.1.3", user="spsadmin", passwd="sps", database="parksys")
    logging.info("mdb set well")
    dbsetup = 1
except:
    logging.warning( "Warning: No database (connection) found. Retry in one minute.")
    dbsetup = 0
    time.sleep(60)
    pass

try:
    client.connect(mqtt_server,mqtt_port, mqtt_alive)
    logging.info("mqtt set well")
    client.loop_start()
except:
    logging.warning("Warning: No broker found. Retry in one minute.")
    log_event(421, "No broker found")
    time.sleep(60)
    pass
client.loop_start()
log_event(120, "mqtt ready")
log_event(121, str({'spaceNumber': len(trig_pin)}))


try:
    while True:
        while dbsetup == 0:
            try: 
                mdb = mysql.connector.connect(host="192.168.1.3", user="spsadmin", passwd="sps", database="parksys")
                dbsetup = 1
            except:
                print( "Warning: No database (connection) found. Retry in one minute.")
                dbsetup = 0
                time.sleep(60)
                pass
        dbsetup = 0

        ent_sendflag, ent_status = get_ent_status(ent_status)
        if ent_sendflag == 1:
            log_event(220, "\"entrance\": \""+str(ent_status)+"\"")
        if get_all_avail() == True:
            avail_to_db(status)

        
        

except KeyboardInterrupt:
    GPIO.cleanup()
    print('end')
