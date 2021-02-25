package myorg

import org.apache.spark.sql.{functions => f}
import org.apache.spark.sql.types.DoubleType


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, Imputer, RobustScaler}
import org.apache.spark.ml.Pipeline
import Array._


import org.apache.log4j.{Level, Logger}
import scala.Array.concat


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



    /* COLUMN SPECIFICATIONS for running pipeline **///TODO parametrize this
    val categorical_features = Array("DISCIPLINE")
    val id_columns = Array("ADOPTION_KEY", "COURSE_KEY", "INSTRUCTOR_GUID", "USER_SSO_GUID")
    val y = "DID_CHURN_19"
    val exclude = Array("DISCIPLINE_CATEGORY")
    val final_exclude = concat(categorical_features, id_columns, Array(y), exclude)



      try{
          /* DATA IMPORT */

          //1. Importing data from S3
          val df = spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load("/Users/spurushe/Downloads/ADOPTION_CHURN_PREDICTION_FEATURES_2_0_0_0.csv")



          /*************************************
          *
          *  PRE PROCESSING
          *
          **************************************/

          // data imported is as strings, so casting the required columns to DoubleType
          val columnsTobeCast = df.columns.filterNot(final_exclude contains _ )

          val df2 = columnsTobeCast.foldLeft(df){ (targetDf, colName) =>
            targetDf.withColumn(colName, f.col(colName).cast(DoubleType))
          }

          /*************************************
          *
          *  CATEGORICAL PIPELINE
          *
          **************************************/
          val features = df.columns.filter( categorical_features contains _ ) // filters out categorical columns

          val strOutputs = for (e <- features) yield e + "_index"
          println("strOutputs......" + strOutputs)
          val stringIndexer = new StringIndexer()
              .setInputCols(features)
              .setOutputCols(strOutputs)

          val oheOutputs = for (e <- features) yield e+ "_ohe"
          println("oheOutputs......" + oheOutputs)
          val ohe = new OneHotEncoder()
              .setInputCols(stringIndexer.getOutputCols)
              .setOutputCols(oheOutputs)

          val catPipeline = new Pipeline().setStages(Array(stringIndexer, ohe))

          df2 = catPipeline.fit(df2).transform(df2)
          println(df2.columns)

          /*************************************
          *
          *  TRAIN TEST SPLIT
          *
          **************************************/
//          var splits = df2.randomSplit(Array(0.8, 0.2), seed = 42)
//          var (trainingData, testData) = (splits(0).cache() , splits(1).cache())
//
//          var numeric_features = df2.columns.filterNot(final_exclude contains _)
//          var imputed_cols = for (e <- numeric_features) yield "IMP_" + e
//          var imp = new Imputer()
//                      .setInputCols(numeric_features)
//                      .setOutputCols(imputed_cols)
//                      .setStrategy("median")
//
//          var imputed_df = imp.fit(trainingData).transform(trainingData).drop(numeric_features: _*)
//
//          println(imputed_df.show(5))


      }
      catch{
        case e: Exception => println("*********************** Exception caught: " + e);
      }
      finally {
        spark.stop()
      }
    }// end of main

}
