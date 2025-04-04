from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common import Time

# 初始化Flink环境
env = StreamExecutionEnvironment.get_execution_environment()

# 添加Kafka依赖
env.add_jars("file:///path/to/flink-sql-connector-kafka_2.12-1.14.4.jar")

# 创建Kafka消费者
kafka_props = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'flink-uv-counter'
}
kafka_consumer = FlinkKafkaConsumer(
    'logs',
    SimpleStringSchema(),
    kafka_props
)

# 添加数据源
stream = env.add_source(kafka_consumer)

# 数据转换
parsed_stream = stream.map(lambda x: x.split()[0], output_type=Types.STRING())

# UV统计
uv_counts = parsed_stream \
    .map(lambda x: (x, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda x: x[0]) \
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10))) \
    .reduce(lambda a, b: (a[0], a[1] + b[1]))

# 输出结果
uv_counts.print()

# 执行任务
env.execute("UV Counter")