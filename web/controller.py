from kafka import KafkaConsumer
import redis
import threading
import time
import json


# kafka_connection redis_host, redis_port: "192.168.31.100:9092", "192.168.31.100", 63790

# Sink data from Kafka to Redis
class Sinker:
    def __init__(self, kafka_connection, redis_host, redis_port):
        self.kafka_connection = kafka_connection
        self.redis_host = redis_host
        self.redis_port = redis_port

    # For counting board and others

    def sink_total_reqs(self):
        host = KafkaConsumer("logv-count-host", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in host:
            time.sleep(1)
            # Total reqs
            value = bytes.decode(msg.value)[1:-1].split(',')
            v = value[1]
            redis_connection.zincrby("count-total-host", "total-req", v)
            print("[SINKER_total_reqs] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_visitor(self):
        host = KafkaConsumer("logv-count-host", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in host:
            time.sleep(1)
            value = bytes.decode(msg.value)[1:-1].split(',')
            k = value[0]
            v = value[1]
            redis_connection.zincrby("count-host", k, v)
            print("[SINKER_visitor] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_reqs(self):
        statuscode = KafkaConsumer("logv-count-statuscode", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in statuscode:  # Valid reqs & not-found reqs
            time.sleep(1)
            value = bytes.decode(msg.value)[1:-1].split(',')
            k = value[0]
            v = value[1]
            redis_connection.zincrby("count-statuscode", k, v)
            print("[SINKER_reqs] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_traffic(self):
        byte_s = KafkaConsumer("logv-count-bytes", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in byte_s:  # Traffic
            time.sleep(1)
            value = bytes.decode(msg.value)
            redis_connection.zincrby("count-bytes", "traffic", value)
            print("[SINKER_traffic] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_log_size(self):
        char = KafkaConsumer("logv-count-char", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in char:  # Log size
            time.sleep(1)
            value = bytes.decode(msg.value)
            redis_connection.zincrby("count-char", "log-size", value)
            print("[SINKER_log_size] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_req_method(self):
        req_method = KafkaConsumer("logv-count-req_method", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in req_method:  # Req method count
            time.sleep(1)
            value = bytes.decode(msg.value)[1:-1].split(',')
            k = value[0]
            v = value[1]
            redis_connection.zincrby("count-req-method", k, v)
            print("[SINKER_req_method] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_req_url(self):
        req_url = KafkaConsumer("logv-count-req_url", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in req_url:
            time.sleep(1)
            value = bytes.decode(msg.value)[1:-1].split(',')
            k = value[0]
            v = value[1]
            redis_connection.zincrby("count-req-url", k, v)
            print("[SINKER_req_url] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_datetime(self):
        datetime = KafkaConsumer("logv-count-datetime", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in datetime:
            time.sleep(1)
            value = bytes.decode(msg.value)[1:-1].split(',')
            k = self.datetime_converter(value[0])
            v = value[1]
            redis_connection.zincrby("count-datetime", k, v)
            print("[SINKER_datetime] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # Datetime formatting (for pattern like: 22/Nov/2018:06:54:33 -> 22/11/2018:06:54:33 -> TIMESTAMP)
    def datetime_converter(self, datetime):
        day = datetime.split(" ")[0].split("/")[0]
        month = datetime.split(" ")[0].split("/")[1]
        rest = datetime.split(" ")[0].split("/")[2]
        mapping = {"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
                   "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12}
        int_datetime = "/".join([str(day), str(mapping[month]), str(rest)])
        return int(time.mktime(time.strptime(int_datetime, "%d/%m/%Y:%H:%M:%S")))  # timestamp

    # For 'Malicious' board
    # raw count
    def sink_intrusion_count(self):
        intrusion = KafkaConsumer("logv-intrusion-count", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in intrusion:
            time.sleep(1)
            value = bytes.decode(msg.value)
            redis_connection.zincrby("count-intrusion", "intrusion-count", value)
            print("[SINKER_intrusion_count] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_normal_count(self):
        normal = KafkaConsumer("logv-normal-count", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in normal:
            time.sleep(1)
            value = bytes.decode(msg.value)
            redis_connection.zincrby("count-normal", "normal-count", value)
            print("[SINKER_normal_count] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # datetime count
    def sink_intrusion_normal_datetime_count(self):
        i_n_datetime_count = KafkaConsumer("logv-intrusion-normal-datetime-count",
                                           bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in i_n_datetime_count:
            time.sleep(1)
            value = eval(msg.value)
            if type(value) is dict:
                datetime = value['datetime']
                intrusion_count = value['intrusion_count']
                normal_count = value['normal_count']
                timestamp = self.datetime_converter(datetime)
                redis_connection.zincrby("count-intrusion-datetime", timestamp, intrusion_count)
                redis_connection.zincrby("count-normal-datetime", timestamp, normal_count)
            if type(value) is tuple:
                for i in value:
                    datetime = i['datetime']
                    intrusion_count = i['intrusion_count']
                    normal_count = i['normal_count']
                    timestamp = self.datetime_converter(datetime)
                    redis_connection.zincrby("count-intrusion-datetime", timestamp, intrusion_count)
                    redis_connection.zincrby("count-normal-datetime", timestamp, normal_count)

    # Intrusion & normal info
    def sink_intrusion(self):
        intrusion = KafkaConsumer("logv-intrusion", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in intrusion:
            time.sleep(1)
            value = bytes.decode(msg.value)
            tupl = eval(value)  # might be dict here
            json_array = json.dumps(tupl)
            redis_connection.publish("intrusion", json_array)
            print("[SINKER_intrusion] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    def sink_normal(self):
        normal = KafkaConsumer("logv-normal", bootstrap_servers=self.kafka_connection)
        redis_connection = redis.Redis(host=self.redis_host, port=self.redis_port, db=0)
        for msg in normal:
            time.sleep(1)
            value = bytes.decode(msg.value)
            tupl = eval(value)  # might be dict here
            json_array = json.dumps(tupl)
            redis_connection.publish("normal", json_array)
            print("[SINKER_normal] " + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))

    # Multi-threaded runner

    def run(self):
        threads = []
        threads.append(threading.Thread(target=self.sink_total_reqs))
        threads.append(threading.Thread(target=self.sink_visitor))
        threads.append(threading.Thread(target=self.sink_reqs))
        threads.append(threading.Thread(target=self.sink_traffic))
        threads.append(threading.Thread(target=self.sink_log_size))
        threads.append(threading.Thread(target=self.sink_req_method))
        threads.append(threading.Thread(target=self.sink_req_url))
        threads.append(threading.Thread(target=self.sink_datetime))
        threads.append(threading.Thread(target=self.sink_intrusion_count))
        threads.append(threading.Thread(target=self.sink_normal_count))
        threads.append(threading.Thread(target=self.sink_intrusion_normal_datetime_count))
        threads.append(threading.Thread(target=self.sink_intrusion))
        threads.append(threading.Thread(target=self.sink_normal))
        for i in threads:
            i.start()