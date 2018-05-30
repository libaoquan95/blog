---
title: Spark 实践——用决策树算法预测森林植被
date: 2018-05-27 13:17:23
categories: Spark实践
tags: 
  - Spark MLlib
  - Scala
description: 
  - 本文基于《Spark 高级数据分析》第4章 用决策树算法预测森林植被集。
---
本文基于《Spark 高级数据分析》第4章 用决策树算法预测森林植被集。
完整代码见 [https://github.com/libaoquan95/aasPractice/tree/master/c4/rdf](https://github.com/libaoquan95/aasPractice/tree/master/c4/rdf)

## 1.获取数据集
> 本 章 用 到 的 数 据 集 是 著 名 的 Covtype 数 据 集， 该 数 据 集 可 以 在 线 下 载（http://t.cn/R2wmIsI），包含一个 CSV 格式的压缩数据文件 covtype.data.gz，附带一个描述数据文件的信息文件 covtype.info。
> 
> 该数据集记录了美国科罗拉多州不同地块的森林植被类型（也就是现实中的森林，这仅仅是巧合！）每个样本包含了描述每块土地的若干特征，包括海拔、坡度、到水源的距离、遮阳情况和土壤类型， 并且随同给出了地块的已知森林植被类型。我们需要总共 54 个特征中的其余各项来预测森林植被类型。
>
> 人们已经用该数据集进行了研究，甚至在 Kaggle 大赛（https://www.kaggle.com/c/forestcover-type-prediction） 中也用过它。本章之所以研究这个数据集， 原因在于它不但包含了数值型特征而且包含了类别型特征。 该数据集有 581 012 个样本，虽然还称不上大数据，但作为一个范例来已经足够大，而且也能够反映出大数据上的一些问题。

下载地址：
1. [http://t.cn/R2wmIsIl](http://t.cn/R2wmIsI) （原书地址）
2. [https://github.com/libaoquan95/aasPractice/tree/master/c4/covtype](https://github.com/libaoquan95/aasPractice/tree/master/c4/covtype)

## 2.数据处理
加载数据
```
val dataDir = "covtype.data"
val dataWithoutHeader = sc.read. option("inferSchema", true).option("header", false). csv(dataDir)
dataWithoutHeader.printSchema
```
![](Spark-实践-用决策树算法预测森林植被/1.png)
![](Spark-实践-用决策树算法预测森林植被/2.png)

结构化数据
```
val colNames = Seq(
    "Elevation", "Aspect", "Slope",
    "Horizontal_Distance_To_Hydrology", "Vertical_Distance_To_Hydrology",
    "Horizontal_Distance_To_Roadways",
    "Hillshade_9am", "Hillshade_Noon", "Hillshade_3pm",
    "Horizontal_Distance_To_Fire_Points"
) ++ (
    (0 until 4).map(i => s"Wilderness_Area_$i")
    ) ++ (
    (0 until 40).map(i => s"Soil_Type_$i")
    ) ++ Seq("Cover_Type")

val data = dataWithoutHeader.toDF(colNames:_*).
    withColumn("Cover_Type", $"Cover_Type".cast("double"))

val Array(trainData, testData) = data.randomSplit(Array(0.9, 0.1))
trainData.cache()
testData.cache()

data.printSchema
```
![](Spark-实践-用决策树算法预测森林植被/3.png)

## 3.构造决策树
构造特征向量
```
val inputCols = trainData.columns.filter(_ != "Cover_Type")
val assembler = new VectorAssembler().setInputCols(inputCols).setOutputCol("featureVector")
val assembledTrainData = assembler.transform(trainData)

val classifier = new DecisionTreeClassifier().
    setSeed(Random.nextLong()).
    setLabelCol("Cover_Type").
    setFeaturesCol("featureVector").
    setPredictionCol("prediction")
```
![](Spark-实践-用决策树算法预测森林植被/4.png)
![](Spark-实践-用决策树算法预测森林植被/5.png)

训练模型
```
val model = classifier.fit(assembledTrainData)
println(model.toDebugString)

model.featureImportances.toArray.zip(inputCols).sorted.reverse.foreach(println)
val predictions = model.transform(assembledTrainData)
predictions.select("Cover_Type", "prediction", "probability").  show(truncate = false)
```
![](Spark-实践-用决策树算法预测森林植被/6.png)
![](Spark-实践-用决策树算法预测森林植被/7.png)
![](Spark-实践-用决策树算法预测森林植被/8.png)

评估模型
```
val evaluator = new MulticlassClassificationEvaluator(). setLabelCol("Cover_Type"). setPredictionCol("prediction")
val accuracy = evaluator.setMetricName("accuracy").evaluate(predictions)
val f1 = evaluator.setMetricName("f1").evaluate(predictions)
println(accuracy)
println(f1)

val predictionRDD = predictions.
    select("prediction", "Cover_Type").
    as[(Double,Double)].rdd
val multiclassMetrics = new MulticlassMetrics(predictionRDD)
println(multiclassMetrics.confusionMatrix)

val confusionMatrix = predictions.
    groupBy("Cover_Type").
    pivot("prediction", (1 to 7)).
    count().
    na.fill(0.0).
    orderBy("Cover_Type")

confusionMatrix.show()
```
![](Spark-实践-用决策树算法预测森林植被/9.png)
![](Spark-实践-用决策树算法预测森林植被/10.png)