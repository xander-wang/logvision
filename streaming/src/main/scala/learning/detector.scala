/*
 * Copyright (c) 2018. Xander Wang. All rights Reserved.
 */

package learning

import org.apache.spark.SparkContext
import org.apache.spark.ml.PipelineModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.dstream.DStream
import processing.log

/*
* @project: logv_streaming
* @description: Apply pre-trained model on streaming urls
* @author: Xander Wang
* @create: 2018-11-21 01:31
*/
object detector {
  def main(log: DataFrame): DataFrame = {

    // Invoke and apply pre-trained model
    val model = PipelineModel.load("/tmp/logvision/ml_model")
    val payload = log.select("host","datetime", "req_url")
      .where("req_url != '/'")
      .toDF("host", "datetime", "data")
    model.transform(payload)
  }
}
