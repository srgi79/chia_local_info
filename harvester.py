import paho.mqtt.client as mqtt
import time, sys, os
from queue import SimpleQueue 
from threading import Thread

harvester = 2
root_dir = '/mnt/'
topic = 'chia/node'
config = {'broker': '', 'port': 1883, 'topic': f'chia/to_h{harvester}', 'id': f'harvester0{harvester}', 'user': '', 'pwd': ''}

# Mqtt Thread
class MQTTPublisher(object):
    def __init__(self, topic, brk, myqueue, sysQueue):
        self.send_topic = topic
        self.brk = brk
        self.myqueue = myqueue
        self.sysQueue = sysQueue
        thread1 = Thread(target=self.run)
        thread1.daemon = True  # Daemonize thread
        thread1.start()  # Start the execution
    
    def on_message(self, client, obj, msg):
        print("Message: " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload.decode("utf-8")))
        if str(msg.payload.decode("utf-8")) == 'list':
            self.sysQueue.put({'cmd': 'list'})

    def run(self):
        self.brk.on_message = self.on_message
        while True:
            if not self.myqueue.empty():
                payload = self.myqueue.get()
                print(f"- MQTTPublisher - run: {payload}")
                if 'send' in payload:
                    info = payload['send']
                    if 'plots' in info and 'totalSize' in info:
                        msg = f"Harvester: 2, Plots: {info['plots']}, Size: {info['totalSize']}"
                        broker.publish(self.send_topic, payload='{'+msg+'}', qos=0, retain=True)
                
            else:
                time.sleep(0.2)

# System Thread
class Sys(object):
    def __init__(self, rootd, myqueue, mqttQueue):
        self.myqueue = myqueue
        self.mqttQueue = mqttQueue
        self.rootd = rootd
        thread2 = Thread(target=self.run)
        thread2.daemon = True  # Daemonize thread
        thread2.start()  # Start the execution

    def getMounts(self):
        dir_list = []
        scanObj = os.scandir(self.rootd)
        for entry in scanObj:
            if entry.is_dir():
                #print(f'Found folder: {entry.name}')
                dir_list.append(entry.name)
        return dir_list

    def getFiles(self, dir_list):
        file_list = []
        total_size = 0
        for dir in dir_list:
            #print('Scanning folder:', dir)
            folder = self.rootd + dir
            scanObj = os.scandir(folder)
            for entry in scanObj:
                if entry.is_file():
                    #print(f'name={entry.name}, path={entry.path}, inode={entry.inode()}, is_dir={entry.is_dir()}, is_file={entry.is_file()}, is_symlink={entry.is_symlink()}, stat={entry.stat()}')
                    file = {'dir': dir, 'name': entry.name, 'size': entry.stat().st_size}
                    total_size += entry.stat().st_size
                    #print(file)
                    file_list.append(file)
        return file_list, total_size
    
    def run(self):
        while True:
            if not self.myqueue.empty():
                payload = self.myqueue.get()
                print(f"- Sys - run: {payload}")
                if 'cmd' in payload:
                    if payload['cmd'] == 'list':
                        dir_list = self.getMounts()
                        print('Folders:', dir_list)
                        files_list, total_size = self.getFiles(dir_list)
                        #print(files_list)
                        print('Number of plots:', len(files_list), 'Total size:', total_size)
                        self.mqttQueue.put({'send': {'plots': len(files_list), 'totalSize': total_size}})
            else:
                # MQTT Sub
                time.sleep(0.5)


def main(argv):
    global broker
    try:

        broker = mqtt.Client( protocol=eval("mqtt.MQTTv311"))
        broker.reinitialise(client_id=config['id'], clean_session=True, userdata=None)

        broker.username_pw_set(config['user'], config['pwd'])
        broker.connect(config['broker'], config['port'], 60)
        broker.subscribe(config['topic'])


        mqttQueue = SimpleQueue()
        sysQueue = SimpleQueue()

        # Start Telegram Bot Thread
        t2 = Sys(root_dir, sysQueue, mqttQueue)
        #Start Publisher Thread
        publish = MQTTPublisher(topic, broker, mqttQueue, sysQueue)

    except Exception as e:
        sys.exit(1)

    # Main work loop
    try:
        rc = broker.loop_start()
        if rc:
            print("Warning: " + str(rc))
        while True:
            time.sleep(1)
        broker.loop_stop() #KeyboardInterrupt
    except Exception as e:
        print("Exception: " + str(e))
        sys.exit(1)


# Get things started
if __name__ == '__main__':
    main(sys.argv[1:])