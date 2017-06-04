package org.pfm.spark.batch

import org.apache.spark.ml.{ PipelineModel, Pipeline }
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature.{ OneHotEncoder, VectorAssembler, StringIndexer, StandardScaler }
import org.apache.spark.ml.linalg.{ Vector, Vectors }

import org.apache.spark.ml.feature.PCA
import scala.util.Random
import org.apache.spark.{ SparkConf, SparkContext }
import org.pfm.spark.utils._
import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession }
//import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._

import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import scala.util.control.Exception.Catch

import org.viz.lightning._
import org.viz.lightning.types._
import org.viz.lightning.Visualization

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.udf

import java.awt.Desktop
import java.net.URI

import org.apache.log4j.Logger
import org.pfm.spark.utils.Configuration

object KMeansBatch {
  val log = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

    val masterKudu: String = Configuration.props.getString("batch.kudu_server")
    val lightning_server: String = Configuration.props.getString("batch.lightning_server")
    val heroku_app: String = Configuration.props.getString("batch.heroku_app")
    val size_point: Int = Configuration.props.getInt("batch.size_point")
    val dataset_delimiter: String = Configuration.props.getString("batch.dataset_delimiter")
    val loglevel: String = Configuration.props.getString("app.loglevel")
    val order_folder: String = Configuration.props.getString("batch.orderFolder")
    val realFraud_folder: String = Configuration.props.getString("batch.realFraud")
    val URLhdfs: String = Configuration.props.getString("app.URLhdfs")

    log.setLevel(org.apache.log4j.Level.toLevel("INFO"))
    log.info(s"start process--------------------------------------------------")

    val spark = SparkSession.builder().appName("KmeansBatch").getOrCreate()

    val k = util.Try(args(0).toInt).getOrElse(4) //Clusters
    val n = util.Try(args(1).toInt).getOrElse(2) // Dimensions
    val folder = util.Try(URLhdfs + args(2)).getOrElse(URLhdfs + "/tmp/staging/in/kschool-topic/17-05-28/14/") // Folder
    ///tmp/staging/in/kschool-topic/17-05-28/14/
    ///tmp/spark/dataset/

    val myDFFile = spark.read.
      option("inferSchema", true).option("delimiter", dataset_delimiter).
      option("header", false).
      csv(folder).
      toDF(
        "id", "time",
        "v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8", "v9", "v10",
        "v11", "v12", "v13", "v14", "v15", "v16", "v17", "v18", "v19", "v20",
        "v21", "v22", "v23", "v24", "v25", "v26", "v27", "v28",
        "amount", "class")

    myDFFile.cache()
    //
    val toInt = udf[Int, String](_.toString().toInt)

    val myTableDF = myDFFile.withColumn("time", toInt(myDFFile("time")))

    log.info(s"call functions process --------------------------------------------------")
    val kMeansFunctions = new org.pfm.spark.utils.Functions(spark)
    val (myDFCalculated, myCentroids) = kMeansFunctions.buildAnomalyDetector(myTableDF, k, n)

    log.info(s"call functions kudu --------------------------------------------------")
    val kuduTable = new org.pfm.kudu.KuduUtils(spark, masterKudu)

    // Clean calculated fields
    val myKuduTable = myDFCalculated.drop("featureVector", "scaledFeatureVector", "pcaScaledFeatureVector")

    kuduTable.writeDataFrameKudu(myKuduTable, "spark_kudu_batchtable", false)
    kuduTable.sqlGetSelectsKudu("spark_kudu_batchtable", order_folder, realFraud_folder, URLhdfs)

    log.info(s"call picture process --------------------------------------------------")
    val pcaArray = myDFCalculated.select("pcaScaledFeatureVector", "cluster").rdd.collect

    buildPicture(size_point, heroku_app, pcaArray, lightning_server, myCentroids, k)

    myDFFile.unpersist()
    log.info(s"end process --------------------------------------------------")
    println("Files Output available on " + order_folder + " + " + realFraud_folder)
    println("SQL Output on realfraud_batch and order_batch")
    
  }

  def getRowX(row: Row): Double = {
    val vector = row.apply(0).asInstanceOf[Vector].toArray
    vector.apply(0)
  }
  def getRowY(row: Row): Double = {
    val vector = row.apply(0).asInstanceOf[Vector].toArray
    vector.apply(1)
  }
  def getCluster(row: Row): Int = {
    val vector = row.apply(1).asInstanceOf[Int]
    vector + 1
  }

  def buildPicture(size_point: Int, heroku_app: String, myArray: Array[Row], myHost: String, myCentroids: Array[Vector], k: Int) {
    val lgn = Lightning(host = myHost)

    lgn.createSession(heroku_app)

    val centerX = myCentroids.map(x => (x.apply(0).asInstanceOf[Double]))
    val centerY = myCentroids.map(x => (x.apply(1).asInstanceOf[Double]))

    val x = myArray.map(getRowX) ++ centerX
    val y = myArray.map(getRowY) ++ centerY
    val group = myArray.map(getCluster) ++ Array.fill(k)(0)

    val viz = lgn.scatter(x, y, group = group, size = size_point)

    println(viz.getPermalinkURL)
    if (Desktop.isDesktopSupported()) {
      Desktop.getDesktop.browse(new URI(viz.getPermalinkURL))
    }
  }

}
