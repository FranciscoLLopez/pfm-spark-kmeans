package org.pfm.spark.stream

import org.apache.kafka.common.serialization.StringDeserializer
import org.pfm.spark.utils.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SparkSession, Row, SQLContext }
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.log4j.Logger
import org.apache.spark.sql.types._
import org.apache.spark.SparkContext

import org.viz.lightning._
import org.viz.lightning.types._
import org.viz.lightning.Visualization

import java.awt.Desktop
import java.net.URI

import org.apache.spark.ml.linalg.{ Vector, Vectors }

object KMeansStream {

  val log = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    val masterKudu: String = Configuration.props.getString("stream.kudu_server")
    val lightning_server: String = Configuration.props.getString("stream.lightning_server")
    val heroku_app: String = Configuration.props.getString("stream.heroku_app")
    val size_point: Int = Configuration.props.getInt("stream.size_point")
    val dataset_delimiter: String = Configuration.props.getString("stream.delimiter")
    val kafka_topic: String = Configuration.props.getString("stream.kafka_topic")
    val kafka_groupID: String = Configuration.props.getString("stream.kafka_groupID")
    val kafka_host: String = Configuration.props.getString("stream.kafka_host")
    val loglevel: String = Configuration.props.getString("app.loglevel")
    val order_folder: String = Configuration.props.getString("stream.orderFolder")
    val realFraud_folder: String = Configuration.props.getString("stream.realFraud")
    val URLhdfs: String = Configuration.props.getString("app.URLhdfs")

    val k = util.Try(args(0).toInt).getOrElse(4) //Clusters
    val n = util.Try(args(1).toInt).getOrElse(2) // Dimensions
    val time = util.Try(args(2).toLong).getOrElse(2L) // Dimensions
    log.setLevel(org.apache.log4j.Level.toLevel(loglevel))
    log.info(s"start process--------------------------------------------------")

    val spark = SparkSession.builder().appName("KMeansStream").getOrCreate()
    val sc = spark.sparkContext

    val ssc = new StreamingContext(sc, Seconds(time))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> kafka_host,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> kafka_groupID,
      "auto.offset.rest" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array(kafka_topic)

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams))

    //stream.map(record => (record.key, record.value)).print
    // Join epoch time & row
    val myStream = stream.map(record => (record.key + dataset_delimiter + record.value))
    var df_stream: DataFrame = null

    val lgn = Lightning(host = lightning_server)
    lgn.createSession(heroku_app)

    var viz: Visualization = null

    var firstTime = true
    val kMeansFunctions = new org.pfm.spark.utils.Functions(spark)
    val kuduTable = new org.pfm.kudu.KuduUtils(spark, masterKudu)
            val mainSchemaStream = StructType(
          //          column name   type       nullable
          StructField("epoch", StringType, true) ::
            StructField("id", IntegerType, true) ::
            StructField("time", IntegerType, true) ::
            StructField("v1", DoubleType, true) ::
            StructField("v2", DoubleType, true) ::
            StructField("v3", DoubleType, true) ::
            StructField("v4", DoubleType, true) ::
            StructField("v5", DoubleType, true) ::
            StructField("v6", DoubleType, true) ::
            StructField("v7", DoubleType, true) ::
            StructField("v8", DoubleType, true) ::
            StructField("v9", DoubleType, true) ::
            StructField("v10", DoubleType, true) ::
            StructField("v11", DoubleType, true) ::
            StructField("v12", DoubleType, true) ::
            StructField("v13", DoubleType, true) ::
            StructField("v14", DoubleType, true) ::
            StructField("v15", DoubleType, true) ::
            StructField("v16", DoubleType, true) ::
            StructField("v17", DoubleType, true) ::
            StructField("v18", DoubleType, true) ::
            StructField("v19", DoubleType, true) ::
            StructField("v20", DoubleType, true) ::
            StructField("v21", DoubleType, true) ::
            StructField("v22", DoubleType, true) ::
            StructField("v23", DoubleType, true) ::
            StructField("v24", DoubleType, true) ::
            StructField("v25", DoubleType, true) ::
            StructField("v26", DoubleType, true) ::
            StructField("v27", DoubleType, true) ::
            StructField("v28", DoubleType, true) ::
            StructField("amount", DoubleType, true) ::
            StructField("class", IntegerType, true) :: Nil)


    myStream.foreachRDD { rdd =>
      {

        val rowRDD = rdd
          .map(_.split(dataset_delimiter))
          .map(attributes => Row(attributes(0),
            attributes(1).toInt,
            attributes(2).toInt,
            attributes(3).toDouble,
            attributes(4).toDouble,
            attributes(5).toDouble,
            attributes(6).toDouble,
            attributes(7).toDouble,
            attributes(8).toDouble,
            attributes(9).toDouble,
            attributes(10).toDouble,
            attributes(11).toDouble,
            attributes(12).toDouble,
            attributes(13).toDouble,
            attributes(14).toDouble,
            attributes(15).toDouble,
            attributes(16).toDouble,
            attributes(17).toDouble,
            attributes(18).toDouble,
            attributes(19).toDouble,
            attributes(20).toDouble,
            attributes(21).toDouble,
            attributes(22).toDouble,
            attributes(23).toDouble,
            attributes(24).toDouble,
            attributes(25).toDouble,
            attributes(26).toDouble,
            attributes(27).toDouble,
            attributes(28).toDouble,
            attributes(29).toDouble,
            attributes(30).toDouble,
            attributes(31).toDouble,
            attributes(32).toInt))

        val myTableDF = spark.createDataFrame(rowRDD, mainSchemaStream).drop("epoch")
        myTableDF.cache()
        //

        //val kMeansFunctions = new org.pfm.spark.utils.Functions(spark)
        val (myDFCalculated, myCentroids) = kMeansFunctions.buildAnomalyDetector(myTableDF, k, n)

        //val kuduTable = new org.pfm.kudu.KuduUtils(spark, masterKudu)
        // Clean calculated fields
        val myKuduTable = myDFCalculated.drop("featureVector", "scaledFeatureVector", "pcaScaledFeatureVector")

        kuduTable.writeDataFrameKudu(myKuduTable, "spark_kudu_streamtable", true)
        kuduTable.sqlGetSelectsKudu("spark_kudu_streamtable", order_folder, realFraud_folder, URLhdfs)

        val pcaArray = myDFCalculated.select("pcaScaledFeatureVector", "cluster").rdd.collect

        val centerX = myCentroids.map(x => (x.apply(0).asInstanceOf[Double]))
        val centerY = myCentroids.map(x => (x.apply(1).asInstanceOf[Double]))
        //
        val x = pcaArray.map(getRowX) ++ centerX
        val y = pcaArray.map(getRowY) ++ centerY

        val group = pcaArray.map(getCluster) ++ Array.fill(k)(0)
        val size = 10

        if (firstTime) {
          viz = lgn.scatterStreaming(x = x, y = y, value = group)
          log.info(s"URL -> " + viz.getPermalinkURL)
          println(viz.getPermalinkURL)
          if (Desktop.isDesktopSupported()) {
            Desktop.getDesktop.browse(new URI(viz.getPermalinkURL))
          }
          firstTime = false
        } else {
          lgn.scatterStreaming(x = x,
            y = y,
            value = group, viz = viz)
        }

        myTableDF.unpersist()

      }
    }

    ssc.start()
    ssc.awaitTermination()
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

}
