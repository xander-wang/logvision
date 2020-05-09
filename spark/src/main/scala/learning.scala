import java.nio.channels.Pipe

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, IDF, NGram, RegexTokenizer, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object learning {
  // 基于逻辑回归分类的URL入侵检测
  // 训练数据集的行格式定义
  case class trainingData(url: String, label: Int)

  // 数据预处理
  def preProcessing(context: SparkContext, session: SparkSession, good_path: String, bad_path: String): Array[DataFrame] = {

    // 载入数据集
    val goodSet = context.textFile(good_path)
    val badSet = context.textFile(bad_path)

    // 添加标记（正常为0，异常为1）
    val goodSetInPair = goodSet.map(line => trainingData(line, 0))
    val badSetInPair = badSet.map(line => trainingData(line, 1))
    val mixedSet:RDD[trainingData] = goodSetInPair ++ badSetInPair

    // 将正常、异常与合并后的数据集转为DataFrame，并返回
    return Array(
      session.createDataFrame(mixedSet).toDF("url", "label"),
      session.createDataFrame(goodSetInPair).toDF("url", "label"),
      session.createDataFrame(badSetInPair).toDF("url", "label"))
  }

  // 学习
  def learning(context: SparkContext, session: SparkSession, good_path: String, bad_path: String, model_path: String): Unit = {

    // 载入处理后的数据
    val dataSet = preProcessing(context, session, good_path, bad_path)(0)

    // 分词
    // 打散为单字符序列
    val regexTokenizer = new RegexTokenizer()
        .setInputCol("url")
        .setOutputCol("char")
        .setPattern("")

    // 得到ngram序列（n=2）
    val ngram = new NGram()
      .setN(2)
      .setInputCol("char")
      .setOutputCol("ngram")

    // 特征提取（TF-IDF）
    // 哈希词频统计（TF）
    val hashingTF = new HashingTF()
      .setInputCol("ngram")
      .setOutputCol("row_feature")
    // 区分程度估算（IDF）
    val idf = new IDF()
      .setInputCol("row_feature")
      .setOutputCol("features")

    // 分类器（逻辑回归），接收label, features列
    val model = new LogisticRegression()
      .setMaxIter(100)
      .setRegParam(0.001)

    // 定义pipeline
    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer, ngram, hashingTF, idf, model))

    // 开始训练，并持久化模型
    val learner = pipeline.fit(dataSet)
    learner.write.overwrite().save(model_path)

  }

  // 测试
  def testing(context: SparkContext, session: SparkSession, good_path: String, bad_path: String, model_path: String): Unit = {

    // 加载测试数据
    val DataSet = preProcessing(context, session, good_path, bad_path)
    val goodDataSet = DataSet(1)
    val badDataSet = DataSet(2)

    // 加载预训练模型
    val model = PipelineModel.load(model_path)

    // 开始测试
    val goodResult = model.transform(goodDataSet)
    val badResult = model.transform(badDataSet)

    // 显示前10条结果，打印准确计数
    goodResult.show(10)
    badResult.show(10)

    val goodLabelCount = goodResult.groupBy("label").count()
    val goodPredictionCount = goodResult.groupBy("prediction").count()
    val badLabelCount = badResult.groupBy("label").count()
    val badPredictionCount = badResult.groupBy("prediction").count()
    goodLabelCount.show(2)
    goodPredictionCount.show(2)
    badLabelCount.show(2)
    badPredictionCount.show(2)
  }

  // 入侵检测
  def applying(model_path: String, logRecord: DataFrame): DataFrame = {
    // 加载预训练模型
    val model = PipelineModel.load(model_path)

    // 接收来自Streaming的url
    // 带空值的payload
    val payload = logRecord
      .select("host","rfc931", "userName", "dateTime", "reqMethod", "reqUrl", "reqProtocol", "statusCode", "bytes")
      .toDF("host", "rfc931", "username", "timestamp", "req_method", "url", "protocol", "status_code", "bytes")
    // 去空值处理后，代入模型得到结果
    model.transform(payload.na.fill(Map("url" -> "")))
  }

  def main(args: Array[String]): Unit = {

    // Spark 初始化
    val sparkConf = new SparkConf()
      .setAppName("logv_learning")
      .setMaster("spark://10.0.0.222:7077")
      .set("spark.driver.allowMultipleContexts", "true")
      .set("spark.executor.cores", "10")
      .set("spark.executor.memory", "30G")
      .set("spark.driver.memory", "30G")
    val sparkContext = new SparkContext(sparkConf)
    val sparkSession = SparkSession.builder()
      .master("10.0.0.222")
      .appName("logv_learning")
      .getOrCreate()

    // 学习
    learning(sparkContext, sparkSession,
      "/home/logv/learning-datasets/training/good.txt",
      "/home/logv/learning-datasets/training/bad.txt",
      "/home/logv/IDModel")

    // 测试
    testing(sparkContext, sparkSession,
      "/home/logv/learning-datasets/testing/good.txt",
      "/home/logv/learning-datasets/testing/bad.txt",
    "/home/logv/IDModel")
  }
}
