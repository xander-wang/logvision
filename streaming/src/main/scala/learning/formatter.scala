/*
 * Copyright (c) 2018. Xander Wang. All rights Reserved.
 */

package learning

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext

/*
* @project: logv_streaming
* @description: Convert url dataset into dataframe
* @author: Xander Wang
* @create: 2018-11-21 00:17
*/
object formatter {
  def getTrainingSet(context: SparkContext, session: SparkSession, good_path: String, bad_path: String): DataFrame = {
    /*
    * @Description: Dataset formatting
    * @Param: [context, session, good_path, bad_path]
    * @return: org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> 
    * @Author: Xander Wang
    * @Date: 2018/11/23 
    */ 
    // Load file
    val good_raw = context.textFile(good_path)
    val bad_raw = context.textFile(bad_path)

    // line => trainingData(data, label)
    // '0' for good, '1' for bad
    val good_pair = good_raw.map(item => trainingData(item, 0))
    val bad_pair = bad_raw.map(item => trainingData(item, 1))

    // trainingData(data, label), ... ,trainingData(data, label) => Seq(trainingData(data, label), ... ,trainingData(data, label))
    val mixed_set:RDD[trainingData] = good_pair ++ bad_pair
    session.createDataFrame(mixed_set).toDF("data", "label")
  }
}
