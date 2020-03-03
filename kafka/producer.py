from json import dumps
import confluent_kafka
import time
import random


class Producer(object):

    def __init__(self):
        self.producer = confluent_kafka.Producer({'bootstrap.servers': 'localhost:9092,ip-10-0-0-8:9092,ip-10-0-0-10:9092',
                                                  'linger.ms': 1, 'batch.num.messages': 3000, 'queue.buffering.max.messages': 10000})
        self.kafka_topic = 'temp_topic'
        self.mu = 32.0
        self.sigma = 0.1

    def create_msg(self, sensor_id, timestamp, millis, val):
        """
        Args:
            sensor_id: int, 
            timestamp: timestamp, 
            millis:int, 
            val:float
        :rtype: dict
        """
        msg = {}
        msg["id"] = sensor_id
        msg["ts"] = timestamp
        msg["millis"] = millis
        msg["val"] = val
        return msg

    def produce_msgs(self):

        while True:
            t = time.strftime('%Y-%m-%dT%H:%M:%S')
            millis = "%.3d" % (time.time() % 1 * 1000)
            for i in range(1, 90):
                sensor_id = i
                signal = random.gauss(self.mu, self.sigma)
                malfunc_sens = [7, 10, 22, 37]
                if i in malfunc_sens:
                    noise = random.expovariate(0.03)
                    final_signal = round((signal + noise), 2)
                else:
                    final_signal = round(signal, 2)
                msg = dumps(self.create_msg(
                    sensor_id, t, millis, final_signal))
                msg = msg.encode('utf-8')
                self.producer.produce(self.kafka_topic, msg)
            self.producer.flush()


if __name__ == "__main__":
    prod = Producer()
    prod.produce_msgs()
