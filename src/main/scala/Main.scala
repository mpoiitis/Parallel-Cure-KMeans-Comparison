/**
  * Created by Zaikis Dimitrios, 8 and Poiitis Marinos, 17 on 13/05/2019.
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions.min

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      println("Parameters needed! Parameters are: number_of_clusters, sample_size (percentage), num_intermediate_clusters, num_representatives, shrink_factor")
    }

    val ss = SparkSession.builder().master("local[*]").appName("BigDataApp").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import ss.implicits._

    // CLI ARGUMENTS
    val numIntermediateClusters = args(2).toInt
    val numClusters = args(0).toInt
    val sampleSize = args(1).toDouble
    val numRepresentatives = args(3).toInt
    val shrinkFactor = args(4).toDouble

    // READ DATA

    val files = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".txt"))

    var data: DataFrame = null
    for (file <- files) {
      val fileDf= ss.read.option("inferSchema","true").csv(file.toString)
      if (data!= null) {
        data= data.union(fileDf)
      } else {
        data= fileDf
      }
    }
    data = data.toDF("x", "y").cache()

    val numOfExamples = data.count()
//    val ratio = 0.01
//    data = data.sample(ratio)

    println("Number of instances in dataset: " + numOfExamples)
//    println(data.count())

//    val data: DataFrame = ss.read.option("inferSchema","true").csv("data/data1.txt").toDF("x", "y").sample(sampleSize).cache()

    // KMEANS

    println("Running KMeans...")
    // creating features column
    val assembler = new VectorAssembler()
      .setInputCols(Array("x","y"))
      .setOutputCol("features")
    val transformedData = assembler.transform(data)

    // the actual model
    val kmeans = new KMeans()
      .setK(numIntermediateClusters)
      .setSeed(1L)
      .setFeaturesCol("features")
    val model = kmeans.fit(transformedData)


    val predictions = model.transform(transformedData)

    // Evaluate clustering by computing Silhouette score
    val evaluator = new ClusteringEvaluator()
    val silhouette = evaluator.evaluate(predictions)
    println("Silhouette with squared euclidean distance = " + silhouette)

    // Shows the result.
    println("KMeans Cluster Centers: ")
    model.clusterCenters.foreach(println)

//    // CURE
//      println("Running SHAS...")
//    // Cluster sample points hierarchically and in a parallel fashion using SHAS
//    val shas = new SHAS(data, splits = 4, ss = ss)
//    val clusters: Array[(Array[Point], Int)] = shas.run(numClusters = numIntermediateClusters)
//    println("Running CURE...")
//    val cure = new Cure(clusters, numClusters, shrinkFactor, numRepresentatives, ss)
//    val finalClusters: RDD[Cluster] = cure.run()
//
//    finalClusters.foreach(println)

    ss.stop()
  }




}
