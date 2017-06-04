package org.pfm.kudu

import org.apache.spark.sql.{ DataFrame, Row, SQLContext, SparkSession }
import org.pfm.spark.utils.Configuration
import org.apache.kudu.spark.kudu._
import org.apache.kudu.client._
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._

class KuduUtils(val spark: SparkSession, mastersKudu:String) {
  val log = Logger.getLogger(getClass.getName)

  def writeDataFrameKudu(myDataframe: DataFrame, kuduTableMain: String, streamMode:Boolean) {

    log.info(s"start process--------------------------------------------------")
    log.setLevel(org.apache.log4j.Level.DEBUG)

    val sqlContext = spark.sqlContext
    val kuduMasters = Seq(mastersKudu).mkString(",")

    // 1. Set schema types

    
    // 2. Define a schema
    val kuduTableMainSchema = StructType(
      //          column name   type       nullable
      StructField("id", IntegerType, false) ::
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
        StructField("class", IntegerType, true) ::
        StructField("cluster", IntegerType, true) ::
        StructField("umbral", DoubleType, true) :: Nil)
    // 3. Se establece la primary key
    val kuduTableMainPrimaryKey = Seq("id")

    // 4. Se establecen numero de replicas y particiones.
    val kuduTableMainOptions = new CreateTableOptions()
    kuduTableMainOptions.
      setRangePartitionColumns(List("id").asJava).
      setNumReplicas(1)

    // Create an instance of a KuduContext
    val kuduContext = new KuduContext(kuduMasters)
    // Check if the table exists, and drop it if it does
    if (!streamMode){
      
        if (kuduContext.tableExists(kuduTableMain)) {
            kuduContext.deleteTable(kuduTableMain) // For schema new changes
        }
    }

    if (!kuduContext.tableExists(kuduTableMain)) {
      kuduContext.createTable(kuduTableMain, kuduTableMainSchema, kuduTableMainPrimaryKey, kuduTableMainOptions)
    }
    //    
    //kuduContext.insertRows(myDataframe, kuduTableMain)
    kuduContext.upsertRows(myDataframe, kuduTableMain)

  }

  def sqlGetSelectsKudu(kuduTableMain: String,topTen_folder:String,realFraud_folder:String,URLhdfs:String) {

    val sqlContext = spark.sqlContext
    val kuduMasters = Seq(mastersKudu).mkString(",")

    val df = sqlContext.read.options(Map("kudu.table" -> kuduTableMain, "kudu.master" -> kuduMasters)).kudu

    df.createOrReplaceTempView(kuduTableMain)
    
    
    val dfOrderUmbral = sqlContext.sql("select id,amount,class,cluster,umbral from "+kuduTableMain+" ORDER BY umbral")
    val dfReal = sqlContext.sql("select id,amount,class,cluster,umbral from "+kuduTableMain+" WHERE class='1'")

    dfOrderUmbral.write.format("com.databricks.spark.csv").mode("Overwrite").option("header", "true").save(URLhdfs+topTen_folder)
    dfOrderUmbral.show()
    
    dfReal.write.format("com.databricks.spark.csv").mode("Overwrite").option("header", "true").save(URLhdfs+realFraud_folder)
    dfReal.show()
    
  }
}
