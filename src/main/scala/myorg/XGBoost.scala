package myorg

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, Imputer, RobustScaler}
import org.apache.spark.ml.Pipeline


import org.apache.log4j.{Level, Logger}


object XGBoost {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val spark = SparkSession.builder
      .master("local[4]")
      .appName("xgboost")
      //.config("spark.driver.memory", "5G")
      //.config("spark.executor.memory", "7G")
      .getOrCreate()


    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration



    /* DATA IMPORT */

      //1. Importing data from S3
      var df = spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Users/spurushe/Documents/data-science-world/input_data/iris_split.csv")

      val categorical_features = Array("Color")
      var features = df.columns.filter( categorical_features contains _ ) // filters out categorical columns
      var encodedFeatures = features.flatMap{ col =>

              val stringIndexer = new StringIndexer()
                .setInputCol(col)
                .setOutputCol(col + "_index")

              val ohe = new OneHotEncoder()
                  .setInputCol(stringIndexer.getOutputCol)
                  .setOutputCol(col + "_ohe")

              Array(stringIndexer, ohe)
            }

      var cat_pipeline = new Pipeline().setStages(encodedFeatures)
      var df2 = cat_pipeline.fit(df).transform(df)

      println(df2.show(5))
      spark.stop()


    }
}
