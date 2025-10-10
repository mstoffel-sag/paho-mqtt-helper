import unittest
from paho_mqtt_helper import mqtt_helper
import logging
import time
import sys

class TestMQTTConnection(unittest.TestCase):
    # Use a public test broker for CI/CD
    broker_host = 'test.mosquitto.org'
    broker_port = 1883

    def setUp(self):
        logging.basicConfig(level=logging.DEBUG, 
                          format='%(asctime)s %(name)s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.topic = 'paho-mqtt-helper/test'
        self.message = 'test_message'
        self.received_message = ''
        
        # Initialize MQTT Helper
        self.mqtth = mqtt_helper.MQTTHelper(
            clientId=f'test-client-{time.time_ns()}',  # Ensure unique client ID
            mqtthost=self.broker_host,
            mqttport=self.broker_port,
            topics=self.topic
        )
        
        # Connect to broker
        self.logger.info(f'Connecting to broker {self.broker_host}')
        result = self.mqtth.connect(self.on_message)
        self.assertEqual(result, 0, "Connection failed")

    def test_publish_and_receive(self):
        """Test basic publish and receive functionality"""
        # Publish with QoS 1
        success = self.mqtth.publish(self.topic, self.message, qos=1)
        self.assertTrue(success, "Failed to publish message")
        
        # Wait for message to be received
        timeout = time.time() + 5  # 5 second timeout
        while time.time() < timeout:
            if self.received_message == self.message:
                break
            time.sleep(0.1)
        
        self.assertEqual(self.message, self.received_message)

    def test_qos0_publish(self):
        """Test QoS 0 publish functionality"""
        result = self.mqtth.publish(self.topic, self.message, qos=0)
        self.assertEqual(result[0], 0, "QoS 0 publish failed")

    def test_invalid_qos(self):
        """Test that invalid QoS values raise ValueError"""
        with self.assertRaises(ValueError):
            self.mqtth.publish(self.topic, self.message, qos=2)

    def on_message(self, client, obj, msg):
        """Callback for received messages"""
        self.received_message = msg.payload.decode('utf-8')
        self.logger.info(f'Received message: {self.received_message}')

    def tearDown(self):
        self.mqtth.disconnect()

if __name__ == '__main__':
    # Allow broker host override from command line
    if len(sys.argv) == 2:
        TestMQTTConnection.broker_host = sys.argv.pop(1)
    unittest.main()
