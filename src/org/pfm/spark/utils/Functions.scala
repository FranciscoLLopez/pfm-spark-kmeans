package org.pfm.spark.utils

import org.apache.spark.ml.{ PipelineModel, Pipeline }
import org.apache.spark.ml.clustering.{ KMeans, KMeansModel }
import org.apache.spark.ml.feature.{ OneHotEncoder, VectorAssembler, StringIndexer, StandardScaler }
import org.apache.spark.ml.linalg.{ Vector, Vectors }
import org.apache.spark.sql.{ DataFrame, SparkSession }
import org.apache.spark.ml.feature.PCA
import scala.util.Random

//class Functions(val spark: SparkSession) {
class Functions(val spark: SparkSession) {
  // Return (String,Vector)
  // The variables are categorized
  import spark.implicits._
  // data = dataframe input
  // K = number of cluster for kmeans
  // n = n dimension to reduce PCA
  def calculusKmeanFit(data: DataFrame, k: Int, n: Int): PipelineModel = {

    val assembler = new VectorAssembler().
      setInputCols(data.columns).
      setOutputCol("featureVector")

    val scaler = new StandardScaler()
      .setInputCol("featureVector")
      .setOutputCol("scaledFeatureVector")
      .setWithStd(true)
      .setWithMean(false)

    val pca = new PCA()
      .setInputCol("scaledFeatureVector")
      .setOutputCol("pcaScaledFeatureVector")
      .setK(n)

    val kmeans = new KMeans().
      setSeed(Random.nextLong()).
      setK(k).
      setPredictionCol("cluster").
      setFeaturesCol("pcaScaledFeatureVector").
      setMaxIter(60).
      setTol(1.0e-5)

    val pipeline = new Pipeline().setStages(Array(assembler, scaler, pca, kmeans))
    pipeline.fit(data)
  }

  // Detect anomalies

  def buildAnomalyDetector(data: DataFrame, k: Int, n: Int): (DataFrame,Array[Vector]) = {
    val pipelineModel = calculusKmeanFit(data, k, n)
    val kMeansModel = pipelineModel.stages.last.asInstanceOf[KMeansModel]
    val centroids = kMeansModel.clusterCenters
    val clustered = pipelineModel.transform(data)
    val datasetumbral = clustered.
      select("id", "pcaScaledFeatureVector", "cluster").
      as[(Int, Vector, Int)].
      map {
        case (id, pcaScaledFeatureVector, cluster) =>
          (id, Vectors.sqdist(centroids(cluster), pcaScaledFeatureVector))
      }

    val dfUmbral = datasetumbral.toDF("id", "umbral")

    val output = clustered.join(dfUmbral, "id")

    (output,centroids)

  }

}

