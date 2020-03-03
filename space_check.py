
import RPi.GPIO as GPIO
import hr04sensor  #/usr/local/lib/python3.7/site-packages/hcsr04sensor/sensor.py
import time
import paho.mqtt.client as mqtt
import mysql.connector

# addr = "CB201"

mqtt_server = "192.168.1.3"
mqtt_port = 1883
mqtt_alive = 60
sub_topic = "sps/CB201/downlink"
pub_topic = "sps/CB201"

ent_trig_pin = 23
ent_echo_pin = 24
trig_pin = [7, 16, 25]
echo_pin = [1, 20, 8]
led = 21

dis_requirement = 40


GPIO.setmode(GPIO.BCM)
GPIO.setup(led, GPIO.OUT)

status = [0] * len(trig_pin)
prev_status = [0] * len(trig_pin)
ent_status = 0

# pub_logevent_topic = "sps/log"


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
    print(str(event)+ " " + event_detail)
    led_blink()

def on_connect(client, userdata, flags, rc):
    # print("Connected mqtt with result code "+str(rc))
    client.subscribe(sub_topic)


def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))


def get_ent_status(ent_status):
    ent_sendflag = False
    ent_prev_status = ent_status
    temp = hr04sensor.Measurement(ent_trig_pin, ent_echo_pin)
    raw_measurement = temp.raw_distance()
    dis = temp.distance_metric(raw_measurement)
    if dis < dis_requirement:
        ent_status = 1
    else:
        ent_status = 0
    if ent_prev_status != ent_status:
        ent_sendflag = True
    return ent_sendflag, ent_status


def get_all_avail():
    sendflag = False
    prev_status = status.copy()
    for i in range(len(trig_pin)):
        temp = hr04sensor.Measurement(trig_pin[i], echo_pin[i])
        raw_measurement = temp.raw_distance()
        dis = temp.distance_metric(raw_measurement)
        # print(int(dis), end=", ")
        if dis > dis_requirement:
            status[i] = 0
        else:
            status[i] = 1
        if prev_status[i] != status[i]:
            sendflag = True
    # print (status)
    prev_status.clear()
    # time.sleep(0.5)
    return sendflag

def avail_to_db(status):
    cur = mdb.cursor()
    dt = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    sql = "INSERT INTO CB201 (Time, "
    for i in range(len(trig_pin)):
        sql = sql + "s" + str(i+1) +", "
    sql = sql.strip(', ') + ") VALUES (%s, " + ("%s, ")*(len(trig_pin)-1) + "%s)"
    temp_val = [dt]
    for i in range(len(trig_pin)):
        temp_val.append(status[i])
    val=tuple(temp_val)
    cur.execute(sql, val)
    mdb.commit()
    log_event(221, "\"availability\": \"" + "".join(str(i) for i in status)+ "\"")
    # print("sent to db: " + str(status))
    led_blink()
    
mainloop = 1
try:
    while mainloop == 1:
        client = mqtt.Client()
        client.on_connect = on_connect
        client.on_message = on_message

        #########################  setup Mysql connection
        dbsetup = 1
        while dbsetup == 1:
            try: 
                # mdb = mysql.connector.connect(db_hostname, db_username, db_password, db_database)
                mdb = mysql.connector.connect(host="192.168.1.3", user="spsadmin", passwd="sps", database="parksys")
                dbsetup = 0
                print("db set well")
            except:
                print( "Warning: No database (connection) found. Retry in one minute.")
                time.sleep(60)
                pass
        
        mqttsetup = 1
        while mqttsetup == 1:
            try:
                client.connect(mqtt_server,mqtt_port, mqtt_alive)
                print("mqtt set well")
                mqttsetup = 0
            except:
                print("Warning: No broker found. Retry in one minute.")
                log_event(421, "No broker found")
                time.sleep(60)
                pass
        
        log_event(120, "ready")
        log_event(121, "\"spaceNumber\": \""+str(len(trig_pin))+"\"")

        #############mainevent
        while mqttsetup == 0:
            mqttsetup = client.loop()
            ent_sendflag, ent_status = get_ent_status(ent_status)
            if ent_sendflag == 1:
                log_event(220, "\"entrance\": \""+str(ent_status)+"\"")
            if get_all_avail() == True:
                avail_to_db(status)


except KeyboardInterrupt:
    mainloop = 0
    GPIO.cleanup()
    print('end')
