'''
Created on 08.02.2022

@author: mstoffel
'''
import logging
import paho.mqtt.client as mqtt
import time

class MQTTHelper(object):
 
    def __init__(self,clientId, mqtthost,mqttport,topics):
        '''
        Initialize vars
        '''
        self.topics= topics
        self.refresh_token_interval = 120
        self.clientId = clientId
        self.ackpub = -1
        self.lastpub = -1
        self.connected = -1
        self.topic_ack=[]
        self.mqtthost = mqtthost
        self.mqttport = mqttport
        self.initialized = True
        self.logger=logging.getLogger(__name__)

    def on_connect(self,client, userdata, flags, rc):
        self.logger.info("on_connect result: " + str(rc))
        self.subscribe_topics(self.topics)
        self.connected=rc

    def check_subs(self):
        wcount=0
        while wcount<10: #wait loop
            self.logger.info('Check Subtopic_ack:' +str(self.topic_ack))
            if len(self.topic_ack)==0:
                self.logger.info('Successfuly Subscribed')
                return True
            time.sleep(1)
            wcount+=1
        return False

    def publish(self,topic,payload,qos=1):
        ret=self.client.publish(topic,payload,qos)
        self.logger.debug('publish ret:' + str(ret))
        return ret

    def on_publish(self,client, obj, mid):
        self.logger.debug("publish: " + str(mid))
        self.ackpub = mid

    def subscribe_topics(self,topics,qos=0):
        self.topic_ack = []
        topics = topics.split(',')
        self.logger.info("topics to subscribe: " +str(topics))
   
        for t in topics:
            try:
                self.logger.debug("Subscribing to topic "+str(t)+" qos: " + str(qos))
                r=self.client.subscribe(t,qos)
                if r[0]==0:
                    self.logger.debug("subscribed to topic "+str(t)+" return code" +str(r) + 'r[1] ' + str(r[1]))
                    self.topic_ack.append(r[1]) #keep track of subscription
                else:
                    self.logger.error("error on subscribing: " + t + ' return code:'+str(r))

            except Exception as e:
                self.logger.error("Exception on subscribe"+str(e))



    def on_subscribe(self,client, obj, mid, granted_qos):
        
        """removes mid valuse from subscribe list"""
        if len(self.topic_ack)==0:
            self.logger.info('Sucessfully  Subscribed')
            return
        for index,t in enumerate(self.topic_ack):
            #self.logger.info('Index: ' + str(index) + ' t:' + str(t) + ' mid:' +str(mid))
            if t==mid:
             #   self.logger.info('Removing sub ' + str(mid))
                self.topic_ack.pop(index)#remove it

    def on_log(self,client, obj, level, string):
        self.logger.debug("on_log: " +string)
    
    def on_disconnect(self,client, userdata, rc):
        self.logger.error("on_disconnect rc: " +str(rc))
      
    def connect(self,on_message):
        self.connected=-1
        ''' Will connect to the mqtt broker
            
            Keyword Arguments:
            on_message -- has to be a method that will be called for new messages distributed to a subscribed topic
            topics -- a list of topics strings like s/ds to subscribe to
        
        ''' 
        if self.initialized == False:
            self.logger.error('Not initialized, please call bootstrap() of edit c8y.properties file. Alternatively you can use cert auth.')
            return

        self.client = mqtt.Client(client_id=self.clientId,clean_session=False)
        
        self.client.on_message = on_message
        self.client.on_publish = self.on_publish
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_subscribe = self.on_subscribe
        self.client.on_log = self.on_log
        self.logger.info('Connecting to: ' + self.mqtthost + ':' + str(self.mqttport) )

        while self.connected == -1:
            self.logger.debug('Waiting for Connect.' + str(self.connected))
            try:
                self.client.connect(self.mqtthost, self.mqttport,keepalive=60)
                self.client.loop_start()
            except Exception as e:
                self.logger.error('Error connection: ' + str(e))
            time.sleep(2.0)

        
        if not self.connected == 0:
            self.logger.debug('Connect not successfull return to client. Code:' + str(self.connected))
            self.client.disconnect()
            return self.connected
        
        if not self.check_subs():
            self.logger.error("Could not subscribe to: " + self.topics)
            return 17
 
        return self.connected  

    def disconnect(self):

        self.logger.info('Disconnect')
        self.client.disconnect()
        self.client.loop_stop()
        self.connected=False

    




        
        
        
