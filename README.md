# LogVision / 使用大数据的分布式实时日志分析与入侵检测系统
[开发文档](https://xander-wang.github.io/2020/05/09/logvision/)
## 版本记录（当前2.0）
- 2018.12.8 v1.0：原型版本，有bug。
- 2020.5.9 v2.0：初步完善版本，可以实现预期效果。
## 简介
LogVision是一个整合了web日志聚合、分发、实时分析、入侵检测、数据存储与可视化的日志分析解决方案。聚合采用Apache Flume，分发采用Apache Kafka，实时处理采用Spark Streaming，入侵检测采用Spark MLlib，数据存储使用HDFS与Redis，可视化采用Flask、SocketIO、Echarts、Bootstrap。

本文下述的使用方法均面向单机伪分布式环境，你可以根据需求进行配置上的调整以适应分布式部署。

本系统各模块由个人独立开发，期间参考了一些有价值的文献与资料。本系统还是个人的本科毕业设计。

获得的奖项：2019年全国大学生计算机设计大赛安徽省二等奖、2019年安徽省信息安全作品赛二等奖。

[原型版本的介绍视频](https://www.bilibili.com/video/BV1eb411T77r)

## 系统架构
![arch](https://github.com/xander-wang/logvision/blob/master/images/arch.png)
## 数据流向
（数字代表处理步骤）  
![dataflow](https://github.com/xander-wang/logvision/blob/master/images/dataflow.png)
## 入侵检测流程
![idsflow](https://github.com/xander-wang/logvision/blob/master/images/idsflow.png)
## 项目结构
- flask：Flask Web后端
- spark：日志分析与入侵检测的实现
- flume：Flume配置文件
- log_gen：模拟日志生成器
- datasets：测试日志数据集
- images：README的图片
## 依赖与版本
- 编译与Web端需要用到的：
  - Java 8, Scala 2.11.12, Python 3.8 (包依赖见requirements), sbt 1.3.8
- 计算环境中需要用到的：
  - Java 8, Apache Flume 1.9.0, Kafka 2.4, Spark 2.4.5, ZooKeeper 3.5.7, Hadoop 2.9.2, Redis 5.0.8
## 使用说明
在开始之前，你需要修改源码或配置文件中的IP为你自己的地址。具体涉及到flume配置文件、Spark主程序、Flask Web后端。 
### 编译Spark应用
在安装好Java8与Scala11的前提下，在```spark```目录下，初始化```sbt```：
```
sbt
```  
退出```sbt shell```并使用```sbt-assembly```对Spark项目进行编译打包：
```
sbt assembly
```
然后将生成的```jar```包重命名为```logvision.jar```。
### 环境准备
你需要一个伪分布式环境（测试环境为CentOS 7），并完成了所有对应版本组件依赖的配置与运行。  
使用```flume```目录下的```standalone.conf```启动一个Flume Agent。  
将```datasets```文件夹中的```learning-datasets```提交如下路径：
```
/home/logv/learning-datasets
```
将```datasets```文件夹中的```access_log```提交如下路径：
```
/home/logv/access_log
```  

### 入侵检测模型训练与测试
提交```jar```包至Spark集群并执行入侵检测模型的生成与测试：
```
spark-submit --class learning logvision.jar
```
你将可以看到如下结果：  
![idoutput](https://github.com/xander-wang/logvision/blob/master/images/idoutput.png)  
两个表格分别代表正常与异常数据集的入侵检测结果，下面四个表格可用于判断识别准确率。如图中所示250条正常测试数据被检测为250条正常，识别率100%；250条异常测试数据被检测为240条异常，10条正常，准确率96%。
### 启动可视化后端
在```flask```目录下执行如下命令，下载依赖包：
```
pip3 install -r requirements.txt
```
启动Flask Web：
```
python3 app.py
```
### 启动实时日志生成器
```log_gen```中的实时日志生成器可根据传入参数（每次写入行数、写入间隔时间）将样本日志中的特定行块追加至目标日志中，以模拟实时日志的生成过程，供后续实时处理。  
```
java log_gen [日志源] [目标文件] [每次追加的行数] [时间间隔（秒）]
```
提交至环境，编译并运行，每2秒将```/home/logv/access_log```文件中的5行追加至```/home/logSrc```中：
```
javac log_gen.java
java log_gen /home/logv/access_log /home/logSrc 5 2
```
### 启动分析任务
提交```jar```包至Spark集群并执行实时分析任务：
```
spark-submit --class streaming logvision.jar
```
### 查看可视化结果
至此你已经完成了后端组件的配置，通过浏览器访问Web端主机的```5000```端口可以查看到实时日志分析的可视化结果：  
欢迎界面：  
![welcome](https://github.com/xander-wang/logvision/blob/master/images/welcome.png)  
实时日志分析界面：  
![analysis](https://github.com/xander-wang/logvision/blob/master/images/analysis.png)  
实时入侵检测界面：  
![id](https://github.com/xander-wang/logvision/blob/master/images/id.png)