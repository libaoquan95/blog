---
title: Spark 实践——基于K均值聚类的网络流量异常检测
date: 2018-06-03 19:08:12
categories: Spark实践
tags: 
  - Spark MLlib
  - Scala
description: 
  - 本文基于《Spark 高级数据分析》第5章 基于K均值聚类的网络流量异常检测
---

## 1.获取数据集
> [KDD Cup](http://www.sigkdd.org/kddcup/index.php) 是一项数据挖掘竞赛，每年由 ACM 特别兴趣小组举办。 KDD Cup 每年都给出一个机器学习问题和相关数据集，研究人员应邀提交论文，论文将详细说明研究人员各自就该机器学习问题给出的最佳方案。1999年 [(http://www.sigkdd.org/kdd-cup-1999-computer-network-intrusion-detection)](http://www.sigkdd.org/kdd-cup-1999-computer-network-intrusion-detection) KDD Cup 竞赛的主题是网络入侵。

下载地址：
1. [http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html](http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html) （原书地址）
2. [https://github.com/libaoquan95/aasPractice/tree/master/c5/kddcup_1999](https://github.com/libaoquan95/aasPractice/tree/master/c5/kddcup_1999)

## 2.数据处理
加载数据
```
    val dataDir = "..\\kddcup_1999\\"

    val data = sc.read.
      option("inferSchema", true).
      option("header", false).
      csv(dataDir+"kddcup.data.corrected").
      toDF(
        "duration", "protocol_type", "service", "flag",
        "src_bytes", "dst_bytes", "land", "wrong_fragment", "urgent",
        "hot", "num_failed_logins", "logged_in", "num_compromised",
        "root_shell", "su_attempted", "num_root", "num_file_creations",
        "num_shells", "num_access_files", "num_outbound_cmds",
        "is_host_login", "is_guest_login", "count", "srv_count",
        "serror_rate", "srv_serror_rate", "rerror_rate", "srv_rerror_rate",
        "same_srv_rate", "diff_srv_rate", "srv_diff_host_rate",
        "dst_host_count", "dst_host_srv_count",
        "dst_host_same_srv_rate", "dst_host_diff_srv_rate",
        "dst_host_same_src_port_rate", "dst_host_srv_diff_host_rate",
        "dst_host_serror_rate", "dst_host_srv_serror_rate",
        "dst_host_rerror_rate", "dst_host_srv_rerror_rate",
        "label")

    data.cache()
```
![](Spark-实践-基于K均值聚类的网络流量异常检测/1.png)

查看每个类的数量
```
data.select("label").groupBy("label").count().orderBy($"count".desc).show(25)
```
![](Spark-实践-基于K均值聚类的网络流量异常检测/2.png)

## 3.构造K-means模型
构造特征向量
```
def clusteringTake(data: DataFrame, k: Int): Unit = {
    val numericOnly = data.drop("protocol_type", "service", "flag").cache()
    val assembler = new VectorAssembler().
      setInputCols(numericOnly.columns.filter(_ != "label")).
      setOutputCol("featureVector")

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("featureVector")

    val pipeline = new Pipeline().setStages(Array(assembler, kmeans))
    val pipelineModel = pipeline.fit(numericOnly)
    val kmeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]

    val withCluster = pipelineModel.transform(numericOnly)

    withCluster.select("cluster", "label").
      groupBy("cluster", "label").count().
      orderBy($"cluster", $"count".desc).
      show(25)

    numericOnly.unpersist()
  }
```
聚类数量为 2 时
```
clusteringTake(data, 2)
```
![](Spark-实践-用决策树算法预测森林植被/4.png)