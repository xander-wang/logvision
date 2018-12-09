/*
 * Copyright (c) 2018. Xander Wang. All rights Reserved.
 */

package processing

import java.util.Properties
import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import com.mongodb.spark.sql._
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

/*
* @project: logv_streaming
* @description: LogVision streaming logic
* @author: Xander Wang
* @create: 2018-09-24 23:50
*/
object streaming {


  def main(args: Array[String]): Unit = {
    /*
    * @Description: Streaming main
    * @Param: [spark_master, mongo_addr_log, mongo_addr_intrusion, kafka_bootstrap_servers, freq]
    *   @Defaults: args(0) = spark_master: "spark://127.0.0.1:7077"
    *              args(1) = mongo_addr_log: "mongodb://127.0.0.1:27017/logv"
    *              args(2) = kafka_bootstrap_servers: "localhost:9092"
    *              args(3) = freq: 3
    * @return: void
    * @Author: Xander Wang
    * @Date: 2018/11/21
    */
    // Create SparkConf and StreamingContext objects
    val sc = new SparkConf()
      .setAppName("logv_streaming")
      .setMaster(args(0))
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.mongodb.input.uri", args(1))
      .set("spark.mongodb.output.uri", args(1))
    val ssc = new StreamingContext(sc, Seconds(args(3).toInt))
    ssc.checkpoint("/tmp/logvision/checkpoint_streaming")
    // MongoDB session
    val ss = SparkSession.builder()
      .master("local")
      .appName("logv_streaming")
      .config("spark.mongodb.input.uri", args(1))
      .config("spark.mongodb.output.uri", args(1))
      .getOrCreate()


    // Initialize Kafka
    // Kafka consumer
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> args(2),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics = Array("logv-source")
    val stream = KafkaUtils.createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))

    // Kafka producer
    class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
      lazy val producer = createProducer()

      def send(topic: String, key: K, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, key, value))

      def send(topic: String, value: V): Future[RecordMetadata] =
        producer.send(new ProducerRecord[K, V](topic, value))
    }
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
    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
      val kafkaProducerConfig = {
        val p = new Properties()
        p.setProperty("bootstrap.servers", args(2))
        p.setProperty("key.serializer", classOf[StringSerializer].getName)
        p.setProperty("value.serializer", classOf[StringSerializer].getName)
        p
      }
      print("kafka producer initialized.")
      ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
    }

    // Reserved for state persistence
      // Streaming processing
      //    // Update function
      //    def mapping_for_pairs(key: String, value: Option[Int], state: State[Int])= {
      //      val sum = value.getOrElse(0) + state.getOption().getOrElse(0)
      //      val output = (key, sum)
      //      state.update(sum)
      //      output
      //    }


    // Processing
    val lines = stream.flatMap { batch =>
      batch.value().split("\n")
    }

    val records = lines.map { record =>
      val PATTERN = """^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+)\s?(\S+)?\s?(\S+)?" (\d{3}|-) (\d+|-)\s?"?([^"]*)"?\s?"?([^"]*)?"?$""".r
      val options = PATTERN.findFirstMatchIn(record)
      val matched = options.get
      log(matched.group(1), matched.group(2), matched.group(3), matched.group(4), matched.group(5), matched.group(6),
        matched.group(7), matched.group(8), matched.group(9), matched.group(10), matched.group(11))
    }

    // Save result to Kafka
      // Full log
      // Character count
      // NCSA key-count
      // Traffic count
    // For on-RDD state persistence
      // .mapWithState(StateSpec.function[String, Int, Int, (String, Int)](mapping_for_pairs _))

    records.foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-log", record.toString)))

    lines.flatMap(rdd => rdd.split("")).count().foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-char", record.toString)))

    records.map(rdd => (rdd.host, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-host", record.toString())))

    records.map(rdd => (rdd.rfc931, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-rfc931", record.toString())))

    records.map(rdd => (rdd.username, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-username", record.toString())))

    records.map(rdd => (rdd.datetime, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-datetime", record.toString())))

    records.map(rdd => (rdd.req_method, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-req_method", record.toString())))

    records.map(rdd => (rdd.req_url, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-req_url", record.toString())))

    records.map(rdd => (rdd.req_protocol, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-req_protocol", record.toString())))

    records.map(rdd => (rdd.statuscode, 1)).reduceByKey(_ + _).foreachRDD(rdd => if(!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-statuscode", record.toString())))

    records.map(rdd => if(rdd.bytes != "-")
      rdd.bytes.toInt else
      0).foreachRDD(rdd => if(!rdd.isEmpty)
        rdd.foreach(record => kafkaProducer.value.send("logv-count-bytes", record.toString)))

    records.map(rdd => (rdd.referrer, 1)).reduceByKey(_ + _).foreachRDD(rdd => if (!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-referrer", record.toString())))

    records.map(rdd => (rdd.user_agent, 1)).reduceByKey(_ + _).foreachRDD(rdd => if (!rdd.isEmpty)
      rdd.foreach(record => kafkaProducer.value.send("logv-count-user_agent", record.toString())))

    // Save logs
    records.foreachRDD { rdd =>
      import ss.implicits._
      val records = rdd.toDF()
      records.write.option("collection", "source").mode("append").mongo()

      // Push & save intrusions and their count
      val result = learning.detector.main(rdd.toDF())
        .select("host","datetime", "data", "prediction")

      val threat = result.where("prediction = 1.0")
      val normal = result.where("prediction = 0.0")

      val threat_count = threat.count()
      val normal_count = normal.count()

      val threat_datetime_count = threat.groupBy("datetime").count().withColumnRenamed("count", "intrusion_count")
      val normal_datetime_count = normal.groupBy("datetime").count().withColumnRenamed("count", "normal_count")
      val threat_normal_datetime_count = threat_datetime_count.join(normal_datetime_count, Seq("datetime"))

      // Push results to kafka & DB
      if ((threat_count != 0) && (normal_count != 0)){
        kafkaProducer.value.send("logv-intrusion", threat.toDF.toJSON.collect().mkString(","))
        kafkaProducer.value.send("logv-normal", normal.toDF.toJSON.collect().mkString(","))

        threat.write.option("collection", "intrusion").mode("append").mongo()
        normal.write.option("collection", "normal").mode("append").mongo()
      }
      kafkaProducer.value.send("logv-intrusion-count", threat_count.toString)
      kafkaProducer.value.send("logv-normal-count", normal_count.toString)
      if (threat_normal_datetime_count.count() != 0){
        kafkaProducer.value.send("logv-intrusion-normal-datetime-count", threat_normal_datetime_count.toDF.toJSON.collect().mkString(","))
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}