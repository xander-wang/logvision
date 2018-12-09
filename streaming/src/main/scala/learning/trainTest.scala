/*
 * Copyright (c) 2018. Xander Wang. All rights Reserved.
 */

package learning

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
* @project: logv_streaming
* @description: ML Testing
* @author: Xander Wang
* @create: 2018-11-23 21:20
*/
object trainTest {
  def main(args: Array[String]): Unit = {
    // Create SparkConf and StreamingContext objects
    val conf = new SparkConf()
      .setAppName("logv_streaming")
      .setMaster(args(0).toString)
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.mongodb.input.uri", args(1).toString)
      .set("spark.mongodb.output.uri", args(1).toString)
    val sc = new SparkContext(conf)
    //val ssc = new StreamingContext(conf, Seconds(args(2).toLong))
    //ssc.checkpoint("/tmp/logvision/checkpoint_streaming")
    // MongoDB session
    val ss = SparkSession.builder()
      .master("local")
      .appName("logv_streaming")
      .config("spark.mongodb.input.uri", args(1).toString)
      .config("spark.mongodb.output.uri", args(1).toString)
      .getOrCreate()

    // ml test
    trainer.main(sc, ss, "/root/ml_data/good_training", "/root/ml_data/bad_training")
    val model = PipelineModel.load("/tmp/logvision/ml_model")
    trainer.tester(model, sc, ss, "/root/ml_data/good_testing", "/root/ml_data/bad_testing")
  }
}
