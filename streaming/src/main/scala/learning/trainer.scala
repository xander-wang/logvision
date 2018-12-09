/*
 * Copyright (c) 2018. Xander Wang. All rights Reserved.
 */

package learning

import org.apache.spark.SparkContext
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.StreamingContext

/*
* @project: logv_streaming
* @description: Learning logic
*   @Reference: https://github.com/faizann24/Fwaf-Machine-Learning-driven-Web-Application-Firewall
* @author: Xander Wang
* @create: 2018-11-21 00:11
*/
object trainer {
  def main(context: SparkContext, session: SparkSession, good_path: String, bad_path: String): Unit = {
    /*
    * @Description:  Fit formatted data into defined pipeline(tokenizer, featurizer, model)
    * @Param: [context, session, good_path, bad_path]
    * @return: void
    * @Author: Xander Wang
    * @Date: 2018-11-27
    */
    val data = formatter.getTrainingSet(context, session, good_path, bad_path)

    // Pipeline definition
    val tokenizer = new Tokenizer()
      .setInputCol("data")
      .setOutputCol("token")
    val hashingTF = new HashingTF()
      .setNumFeatures(100000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val model = new LogisticRegression()
      .setMaxIter(100000)
      .setRegParam(0.00001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer, hashingTF, model))

    // Fit data into model, then persist the model
    val live_model = pipeline.fit(data) // data: trainingData[data, label, token, features]
    live_model.write.overwrite.save("/tmp/logvision/ml_model")
  }

  def tester(model: PipelineModel, context: SparkContext, session: SparkSession, good_testing_data_path: String, bad_testing_data_path: String): Unit ={

    /*
    * @Description: Model Tester
    * @Param: [model, context, session, good_testing_data_path, bad_testing_data_path]
    * @return: void
    * @Author: Xander Wang
    * @Date: 2018-11-27
    */
    val test = formatter.getTrainingSet(context, session, good_testing_data_path, bad_testing_data_path)

    val res = model.transform(test)
    res.show(10000)

  }
}