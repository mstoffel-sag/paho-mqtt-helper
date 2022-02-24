
import unittest
from  paho_mqtt_helper import mqtt_helper
import logging
import time
import sys



class TestMQTTConnection(unittest.TestCase):
    broker_host = 'localhost'
    def setUp(self):

        logging.basicConfig(level=logging.INFO,format='%(asctime)s %(name)s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.topic = 'my_topic'
        self.message = 'my_message'
        self.received_message =  ''
        self.logger.info("")
        self.mqtth = mqtt_helper.MQTTHelper('my_client_id',self.broker_host,1883,self.topic)
        self.logger.info('Connectint to the broker')
        self.mqtth.connect(self.on_message)

    def test_echo(self):
        self.mqtth.publish(self.topic,self.message)
        time.sleep(2)
        self.assertEqual(self.message,self.received_message)

    def on_message(self,client, obj, msg):
        self.received_message = msg.payload.decode('utf-8')
        self.logger.info('New Message: ' + self.received_message)

    def tearDown(self):
        self.mqtth.disconnect()


if __name__ == '__main__':
   
    if len(sys.argv) == 2:
        TestMQTTConnection.broker_host = sys.argv[1]
        del sys.argv[1:]
    unittest.main()
