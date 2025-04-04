import os
import time
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from kafka import KafkaProducer

# 初始化OpenTelemetry
trace.set_tracer_provider(TracerProvider())
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer(__name__)

# Kafka生产者配置
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# 监控日志文件变化
def watch_logs(directory):
    known_files = {}
    while True:
        for filename in os.listdir(directory):
            filepath = os.path.join(directory, filename)
            if os.path.isfile(filepath):
                current_size = os.path.getsize(filepath)
                if filepath not in known_files or known_files[filepath] < current_size:
                    known_files[filepath] = current_size
                    with open(filepath, 'r') as f:
                        f.seek(known_files[filepath])
                        new_data = f.read()
                        if new_data:
                            with tracer.start_as_current_span('log_collection'):
                                producer.send('logs', new_data.encode('utf-8'))
        time.sleep(1)

if __name__ == '__main__':
    watch_logs('/var/log/myapp')