import java.text.SimpleDateFormat
import java.util.concurrent.Future
import java.util.{Locale, Properties}

import com.redis.RedisClientPool
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.util.matching.Regex

object streaming extends {

  // 日志格式
  case class log(host: String,
                 rfc931: String,
                 userName: String,
                 dateTime: String,
                 reqMethod: String,
                 reqUrl: String,
                 reqProtocol: String,
                 statusCode: String,
                 bytes: String)

  // 重写Kafka生产方法
  class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
    lazy val producer: KafkaProducer[K, V] = createProducer()

    def send(topic: String, key: K, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, key, value))

    def send(topic: String, value: V): Future[RecordMetadata] =
      producer.send(new ProducerRecord[K, V](topic, value))
  }
  
  // Kafka生产者初始化，生产分析结果
  object KafkaSink {
    import scala.collection.JavaConversions._
    def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
      val createProducerFunc = () => {
        val producer = new KafkaProducer[K, V](config)
        sys.addShutdownHook {
          producer.close()
        }
        producer
      }
      new KafkaSink(createProducerFunc)
    }

    def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
  }

  // 流式计算逻辑
  def streaming(kafkaStream: InputDStream[ConsumerRecord[String, String]],
                kafkaProducer: Broadcast[KafkaSink[String, String]],
                sparkSession: SparkSession, sparkContext: SparkContext): Unit = {

    // 按行分割每批日志
    val lines = kafkaStream.flatMap { batch =>
      batch.value().split("\n")
    }

    // 用正则匹配将日志格式化，并同时完成日期时间转时间戳
    val simpleDateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", Locale.ENGLISH)

    val logRecords = lines.map { row =>
      val pattern: Regex = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r
      val options = pattern.findFirstMatchIn(row)
      if (options.isDefined) {
        // 若匹配，返回格式化日志结构
        val matched = options.get
        log(matched.group(1), matched.group(2), matched.group(3),
          String.valueOf(simpleDateFormat.parse(matched.group(4)).getTime),
          matched.group(5), matched.group(6), matched.group(7), matched.group(8), matched.group(9))
      }
      else {
        // 若不匹配，用空值跳过
        log("foo", "foo", "foo", "0", "foo", "foo", "foo", "foo", "0")
      }
    }

    // 对每个日志字段计数
    // 行数统计
    val logLineCount = logRecords
      .count()
      .map(rdd => ("count", rdd.toInt))

    // 日志大小统计（KB）
    val logSize = logRecords
      .flatMap(rdd => rdd.toString.split(""))
      .count()
      .map(count => ("sum", (count / 1024).toInt))

    // 请求IP统计
    val hostCount = logRecords
      .map(rdd => (rdd.host, 1))
      .reduceByKey(_ + _)

    // 身份统计
    val rfc931Count = logRecords
      .map(rdd => (rdd.rfc931, 1)).reduceByKey(_ + _)

    // 用户名统计
    val userNameCount = logRecords
      .map(rdd => (rdd.userName, 1)).reduceByKey(_ + _)

    // 时间戳统计
    val dateTimeCount = logRecords
      .map(rdd => (rdd.dateTime, 1))
      .reduceByKey(_ + _)

    // 请求方式统计
    val reqMethodCount = logRecords
      .map(rdd => (rdd.reqMethod, 1))
      .reduceByKey(_ + _)

    // 请求资源统计
    val reqUrlCount = logRecords
      .map(rdd => (rdd.reqUrl, 1))
      .reduceByKey(_ + _)

    // 请求协议统计
    val reqProtocolCount = logRecords
      .map(rdd => (rdd.reqProtocol, 1))
      .reduceByKey(_ + _)

    // 状态码统计
    val statusCodeCount = logRecords
      .map(rdd => (rdd.statusCode, 1))
      .reduceByKey(_ + _)

    // 流量统计（MB）
    val bytesSum = logRecords
      .map { rdd =>
        if (rdd.bytes != "-")
          ("sum", rdd.bytes.toInt / 1024 / 1024)
        else
          ("sum", 0)
      }
      .reduceByKey(_ + _)

    // 创建Redis连接池，延迟初始化
    lazy val redisClients = new RedisClientPool("10.0.0.222", 6379)

    // Redis持久化逻辑，使用有序集合（ZSET），对于重复记录每次更新分数以实现结果累加
    def saveToRedis(rdd: RDD[(String, Int)], key: String): Unit = {
      rdd.foreachPartition { part =>
        // 对每个Partition创建一个连接
        val con = redisClients.withClient(client => client)
        // 对Partition中的每个记录进行持久化
        part.foreach { each => {
          try {
            if (con != null && each != null) {
              con.zincrby(key, each._2, each._1)
            }
          } catch {
            case e: Exception => print(e)
          }
        }
        }
        // 关闭连接，释放资源
        con.close()
      }
    }

    // 结果持久化至Redis
    logLineCount.foreachRDD(rdd => saveToRedis(rdd, "line"))
    logSize.foreachRDD(rdd => saveToRedis(rdd, "size"))
    hostCount.foreachRDD(rdd => saveToRedis(rdd, "host"))
    rfc931Count.foreachRDD(rdd => saveToRedis(rdd, "rfc931"))
    userNameCount.foreachRDD(rdd => saveToRedis(rdd, "username"))
    dateTimeCount.foreachRDD(rdd => saveToRedis(rdd, "datetime"))
    reqMethodCount.foreachRDD(rdd => saveToRedis(rdd, "reqmt"))
    reqUrlCount.foreachRDD(rdd => saveToRedis(rdd, "url"))
    reqProtocolCount.foreachRDD(rdd => saveToRedis(rdd, "proto"))
    statusCodeCount.foreachRDD(rdd => saveToRedis(rdd, "statcode"))
    bytesSum.foreachRDD(rdd => saveToRedis(rdd, "traffic"))

    // 入侵检测、分析结果队列化与HDFS存储
    logRecords.foreachRDD { rdd =>

      import sparkSession.implicits._

      // 应用到模型
      val result = learning.applying("/home/logv/IDModel", rdd.toDF())
        .select("host", "rfc931", "username", "timestamp", "req_method", "url", "protocol",
          "status_code", "bytes", "probability", "prediction")
      // 正常请求结果
      val goodResult = result.where("prediction = 0.0")
      // 异常请求结果
      val badResult = result.where("prediction = 1.0")

      // 分类计数
      val goodCount = Seq(("good", goodResult.count().toInt))
        .toDF("good", "count").rdd
        .map(row => (row.getString(0), row.getInt(1)))
      saveToRedis(goodCount, "good")

      val badCount = Seq(("bad", badResult.count().toInt))
        .toDF("bad", "count").rdd
        .map(row => (row.getString(0), row.getInt(1)))
      saveToRedis(badCount, "bad")

      // 按时间顺序的分类计数
      val goodCountPerTimestamp = goodResult
        .groupBy("timestamp")
        .count()
        .withColumnRenamed("count", "goodCount")
        .rdd.map(row => (row.getString(0), row.getLong(1).intValue()))
      saveToRedis(goodCountPerTimestamp, "goodts")

      val badCountPerTimestamp = badResult
        .groupBy("timestamp")
        .count()
        .withColumnRenamed("count", "badCount")
        .rdd.map(row => (row.getString(0), row.getLong(1).intValue()))
      saveToRedis(badCountPerTimestamp, "badts")

      // 将分析结果生产至Kafka
      kafkaProducer.value.send("good_result", goodResult.toDF().toJSON.collectAsList().toString)
      kafkaProducer.value.send("bad_result", badResult.toDF().toJSON.collectAsList().toString)

      // 将分析结果存储到HDFS
      result.write.format("json").mode(SaveMode.Append).save("hdfs://localhost:9000/logv")

    }

  }

  def main(args: Array[String]): Unit = {

    // Spark初始化
    val sparkConf: SparkConf = new SparkConf()
      .setAppName("logv_streaming")
      .setMaster("spark://10.0.0.222:7077")
      .set("spark.driver.allowMultipleContexts", "true")
    val sparkContext = new SparkContext(sparkConf)
    val streamingContext = new StreamingContext(sparkContext, Seconds("1".toInt))
    val sparkSession = SparkSession.builder()
      .master("10.0.0.222")
      .appName("logv_streaming")
      .getOrCreate()
    // 检查点
    streamingContext.checkpoint("/home/logv/streaming_checkpoint")


    // Kafka消费者初始化，消费来自Flume的数据流
    val kafkaConsumerParams = Map[String, Object](
      "bootstrap.servers" -> "10.0.0.222:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "logv-ng_flumeToSpark",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("raw_log")
    val stream = KafkaUtils.createDirectStream(streamingContext, PreferConsistent, Subscribe[String, String](topics, kafkaConsumerParams))

    // 创建Kafka生产者
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", "10.0.0.222:9092")
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      streamingContext.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }



    // 开始日志分析
    streaming(stream, kafkaProducer, sparkSession, sparkContext)


    streamingContext.start()
    streamingContext.awaitTermination()

  }
}