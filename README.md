# LogVision / 实时web访问日志分析与入侵检测系统
2018.12.8 v1.0.0
## 简介

LogVision是一套整合了web日志聚合、分发、实时处理、入侵检测、数据缓存和持久化与可视化的日志分析解决方案。其中，聚合采用Apache Flume，分发采用Apache Kafka，实时处理采用Spark Streaming，入侵检测采用Spark MLlib库，数据持久化使用MongoDB，缓存使用Redis，webapp与可视化采用Flask, Socket.IO, Echarts等，前端框架使用Bootstrap。

本系统由作者独立开发，属于个人学习与研究性项目，最初版本并非面向生产环境构建。

衷心感谢在100天的研发周期内帮助过作者的社区内容作者。

## 使用的组件版本

Apache Flume: 1.8.0

Apache Kafka: 2.11-2.0.0

Apache Spark: 2.3.1

Python package: (已发现兼容性问题，需匹配以下版本）

kafka-python==1.4.3

redis==2.10.6

## 项目结构

streaming: Spark的分析与入侵检测（Scala, sbt)

web: Flask项目

log_gen: 模拟日志生成脚本

logvision.conf: Flume配置文件

## 数据流向

(原始日志数据)---->Flume---Kafka--->Spark(--->MongoDB)---Kafka--->Flask---Redis--->web(Socket.IO, Echarts)<--访问

## 系统架构与实现

详见作者博客。

## 目前存在的问题

由于ID部分采用逻辑回归对来源进行甄别，误报率仍较高，有待优化；

代码碎片化较严重；

数据处理延时较高，Flask多线程性能不佳导致体验较差；

潜在bug；


# LogVision / Real-time Web Access Log Analysis & Intrusion Detection System
2018.12.8 v1.0.0
## Briefing

LogVision is a web access log analysis solution that integrates features such as log aggregation(Apache Flume), distribution(Apache Kafka), real-time analysis(Spark Streaming), intrusion detection(Spark MLlib), data caching(Redis), persistence(MongoDB), visualization(Flask, Socket.IO, Echarts), etc. Furthermore, Bootstrap is used for front-end pages.

The project is developed by the author himself, and it's a learning & research project, not production-oriented.

Special thanks to those community bloggers who helped the author during 100 days of the dev cycle.

## Used Components

Apache Flume: 1.8.0

Apache Kafka: 2.11-2.0.0

Apache Spark: 2.3.1

Python package: (Due to compatiblity issue, please use following version）

kafka-python==1.4.3

redis==2.10.6

## Project Structure

streaming: Spark analysis & ID (Scala, sbt)

web: Flask project

log_gen: Log generator

logvision.conf : Flume config file

## Dataflow

(Raw Access Log)---->Flume---Kafka--->Spark(--->MongoDB)---Kafka--->Flask---Redis--->web(Socket.IO, Echarts)<--visitor

## System Arch. & Impl.

Please refer to the project author's blog.

## Problems

Since logistic regression has been implemented as the main ID method, actual fitting accuracy is not satisfying;

Code fragmentation;

Due to the processing time lag, the system is not actually real 'real-time', and poor multi-threaded performance(Flask) in the initial build.

Potential bugs;
