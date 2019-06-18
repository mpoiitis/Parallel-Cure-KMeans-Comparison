/**
  * Created by Zaikis Dimitrios, 8 and Poiitis Marinos, 17 on 13/05/2019.
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.rdd.RDD
import java.io._
import sys.process._

import scala.collection.mutable.ListBuffer

object Main {

  println("Valid CLI arguments are: \n" +
    "number_of_clusters, sample_size (percentage), num_intermediate_clusters, num_representatives, shrink_factor," +
    " from_python (boolean), withRepresentatives (boolean), merge (boolean)")

  // CLI ARGUMENTS
  var numIntermediateClusters = 10
  var numClusters = 5
  var sampleSize = 0.001
  var numRepresentatives = 10
  var shrinkFactor = 0.2
  var from_python = true
  var withRepresentatives = false
  var merge = true
  var filepath = "data/"
  var pyFilePath = "C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\main.py"
  val ss: SparkSession = SparkSession.builder().master("local[*]").appName("BigDataApp").getOrCreate()
  Logger.getRootLogger.setLevel(Level.WARN)
  import ss.implicits._

  def main(args: Array[String]): Unit = {
    // CLI ARGUMENTS
    numClusters = if (args.length >= 1) args(0).toInt else 5
    sampleSize = if (args.length >= 2) args(1).toDouble else 0.001
    numIntermediateClusters = if (args.length >= 3) args(2).toInt else 10
    numRepresentatives = if (args.length >= 4) args(3).toInt else 10
    shrinkFactor = if (args.length >= 5) args(4).toDouble else 0.2
    from_python = if (args.length >= 6) args(5).toBoolean else true
    withRepresentatives = if (args.length >= 7) args(6).toBoolean else false
    merge = if (args.length >= 8) args(7).toBoolean else true
    filepath = if (args.length >= 9) args(8) else "data/"
    pyFilePath = if (args.length >= 10) args(9) else "C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\main.py"

    println("numIntermediateClusters: " + numIntermediateClusters)
    println("numClusters: " + numClusters)
    println("numRepresentatives: " + numRepresentatives)
    println("sampleSize: " + sampleSize)
    println("shrinkFactor: " + shrinkFactor)
    println("from_python: " + from_python)
    println("withRepresentatives: " + withRepresentatives)
    println("merge: " + merge)
    println("filepath " + filepath)
    // READ DATA
   
    val files = new java.io.File(filepath).listFiles.filter(_.getName.endsWith(".txt"))

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

    println("Number of instances in dataset: " + numOfExamples)

    var start: Long = 0
    var end: Long = 0

    // KMEANS

    println("Running KMeans...")

    start = System.currentTimeMillis()

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
    val evaluator = new ClusteringEvaluator().
      setPredictionCol("prediction").
      setFeaturesCol("features").
      setMetricName("silhouette")

    end = System.currentTimeMillis()
    println("Total time (KMeans): " + (end - start) + " ms")

    var file: File = null

    // STORE KMEANS CLUSTERS IN FILE FOR PYTHON CLUSTERING AND SILHOUETTE EXTRACTION

    if (from_python) {
      file = new File("produced_data/KMeansClusters")
      if (file.exists()) {
        println("Directory exists, deleting...")
        this.delete(file)
      }
      println("Writing KMeans centers to file")
      ss.sparkContext.parallelize(model.clusterCenters).repartition(1).saveAsTextFile("produced_data/KMeansClusters")
    }

    // POST PROCESSING

    // if we are not using python script, run SHAS for post-processing
    if (!from_python) {
      println("Running SHAS for post processing ...")

      start = System.currentTimeMillis()

      val dataForHierarchical = ss.sparkContext.parallelize(model.clusterCenters).map(vector => (vector.toArray(0), vector.toArray(1))).toDF("x", "y").cache()
      val shasPost = new SHAS(dataForHierarchical, splits = 4, ss = ss)
      val serialClusters: Array[(Array[Point], Int)] = shasPost.run(numClusters = numClusters,
        fileLocation = "produced_data/subgraphIdsPostProcess",
        dataParτitionFilesLocation = "produced_data/dataPartitionsPostProcess")

      end = System.currentTimeMillis()
      println("Total time (SHAS Post-process): " + (end - start) + " ms")

      val clusters = ss.sparkContext.parallelize(serialClusters)
        .map { case (points: Array[Point], id: Int) => (this.meanPoint(points).dimensions, id) }
        .map { case (arr: Array[Double], id: Int) => (arr, id) }
        .toDF("features", "prediction").as[(Array[Double], Int)].collect()

      val bClusters = ss.sparkContext.broadcast(clusters)

      val predictions = data
        .map(row => (row.getDouble(0), row.getDouble(1),
          Array(row.getDouble(0), row.getDouble(1)), this.closestCluster(row.getDouble(0), row.getDouble(1), bClusters.value)))
        .toDF("x", "y", "features", "prediction")

      val silhouette = evaluator.evaluate(predictions)
      println("Silhouette with squared euclidean distance = " + silhouette)
    }
    else {
      println("Running Hierarchical Clustering for post processing using python script")
      val result = s"python $pyFilePath postProcess $numClusters" ! ProcessLogger(stdout append _, stderr append _)
      println("Result: " + result)
    }

    // ========================================================== 2ND PART ==========================================================

    // SAMPLE DATA FOR SHAS PRE PROCESSING

    val dataSample = data.sample(sampleSize).cache()

    // SAVE FILE NEEDED FROM PYTHON SCRIPT TO CREATE INTERMEDIATE CLUSTERS

    if (from_python) {
      file = new File("produced_data/sampleFromData")
      if (file.exists()) {
        println("Directory exists, deleting...")
        this.delete(file)
      }
      println("Writing sample to file")
      dataSample.rdd.repartition(1).saveAsTextFile("produced_data/sampleFromData")
    }

    // CURE
    if (merge){
      this.cureWithMerge(data, dataSample)
    }
    else {
      this.cureWithoutMerge(data, dataSample)
    }

    ss.stop()
  }

  def cureWithMerge(data: DataFrame, dataSample: DataFrame): Unit ={
    var start: Long = 0
    var end: Long = 0
    var clusters2: Array[(Array[Point], Int)] = null
    if (!from_python) {
      println("Running SHAS for pre processing ...")

      start = System.currentTimeMillis()

      // Cluster sample points hierarchically and in a parallel fashion using SHAS
      val shasPre = new SHAS(dataSample, splits = 4, ss = ss)
      clusters2 = shasPre.run(numClusters = numIntermediateClusters)

      end = System.currentTimeMillis()
      println("Total time (SHAS Pre-process): " + (end - start) + " ms")
    }
    else {
      println("Running Hierarchical Clustering for pre processing using python script")
      val result1 = s"python C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\main.py preProcess $numIntermediateClusters" ! ProcessLogger(stdout append _, stderr append _)
      println("Result: " + result1)

      // if error occurred do not continue
      if (result1 != 0) {
        return
      }
      import scala.io.Source
      val filename = "C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\pythonData\\intermediateClusters.txt"
      var counter = 0
      var map: Map[Int, ListBuffer[Point]] = Map()
      for (line <- Source.fromFile(filename).getLines) {
        val arr = line.split(" ")
        val x = arr(0).toDouble
        val y = arr(1).toDouble
        val point: Point = Point(Array(x, y), counter)
        counter += 1

        val clusterId = arr(2).toInt

        if (!map.contains(clusterId)){
          map += (clusterId -> ListBuffer(point))
        }
        else {
          map = map.get(clusterId) match {
            case Some(xs:ListBuffer[Point]) => map.updated(clusterId, point +: xs)
            case None => map
          }
        }
      }

      // convert map of key: clusterId, value: List[Points] -> Array[(Array[Point], Int)]
      val tempList: ListBuffer[(Array[Point], Int)] = ListBuffer()
      map.keys.foreach(clusterId => tempList += Tuple2(map(clusterId).toArray, clusterId))

      clusters2 = tempList.toArray
    }

    println("Running CURE...")

    start = System.currentTimeMillis()

    val cure = new Cure(clusters2, numClusters, shrinkFactor, numRepresentatives, ss)
    val finalClusters: RDD[Cluster] = cure.run()

    end = System.currentTimeMillis()
    println("Total time (CURE): " + (end - start) + " ms")

    finalClusters.foreach(println)

    // CURE SILHOUETTE
    val evaluator = new ClusteringEvaluator().
      setPredictionCol("prediction").
      setFeaturesCol("features").
      setMetricName("silhouette")

    // calculate Silhouette score using representatives
    if (withRepresentatives){
      println("Calculate Silhouette score using representatives")

      val clustersToPoints: Array[(Point, Int)] = finalClusters
        .map(cluster => cluster.points.map{point: Point => (point, cluster.id)})
        .flatMap(l => l).collect()

      val bFinalClusters = ss.sparkContext.broadcast(clustersToPoints)

      val newPredictions = data
        .map(row => (row.getDouble(0), row.getDouble(1),
          Array(row.getDouble(0), row.getDouble(1)),
          this.closestCluster(row.getDouble(0),
            row.getDouble(1),
            bFinalClusters.value.map{case (point, id) => (point.dimensions, id)})))
        .toDF("x", "y", "features", "prediction")

      val silhouette = evaluator.evaluate(newPredictions)
      println("Silhouette with squared euclidean distance for CURE = " + silhouette)
    } // calculate Silhouette score using cluster centers
    else {
      println("Calculate Silhouette score using cluster centers")

      val clustersForEvaluation = finalClusters
        .map { cluster: Cluster => (this.meanPoint(cluster.points).dimensions, cluster.id) }
        .toDF("features", "prediction").as[(Array[Double], Int)].collect()

      val bFinalClusters = ss.sparkContext.broadcast(clustersForEvaluation)

      val newPredictions = data
        .map(row => (row.getDouble(0), row.getDouble(1),
          Array(row.getDouble(0), row.getDouble(1)),
          this.closestCluster(row.getDouble(0),
            row.getDouble(1),
            bFinalClusters.value)))
        .toDF("x", "y", "features", "prediction")

      val silhouette = evaluator.evaluate(newPredictions)
      println("Silhouette with squared euclidean distance for CURE = " + silhouette)
    }
  }

  def cureWithoutMerge(data: DataFrame, dataSample: DataFrame): Unit ={
    var start: Long = 0
    var end: Long = 0
    var clusters2: Array[(Array[Point], Int)] = null
    if (!from_python) {
      println("Running SHAS for pre processing ...")

      start = System.currentTimeMillis()

      // Cluster sample points hierarchically and in a parallel fashion using SHAS
      val shasPre = new SHAS(dataSample, splits = 4, ss = ss)
      clusters2 = shasPre.run(numClusters = numClusters)

      end = System.currentTimeMillis()
      println("Total time (SHAS Pre-process): " + (end - start) + " ms")
    }
    else {
      println("Running Hierarchical Clustering for pre processing using python script")
      val result1 = s"python C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\main.py preProcess $numClusters" ! ProcessLogger(stdout append _, stderr append _)
      println("Result: " + result1)

      // if error occurred do not continue
      if (result1 != 0) {
        return
      }
      import scala.io.Source
      val filename = "C:\\Users\\Marinos\\IdeaProjects\\CURE-algorithm\\src\\main\\python\\pythonData\\intermediateClusters.txt"
      var counter = 0
      var map: Map[Int, ListBuffer[Point]] = Map()
      for (line <- Source.fromFile(filename).getLines) {
        val arr = line.split(" ")
        val x = arr(0).toDouble
        val y = arr(1).toDouble
        val point: Point = Point(Array(x, y), counter)
        counter += 1

        val clusterId = arr(2).toInt

        if (!map.contains(clusterId)){
          map += (clusterId -> ListBuffer(point))
        }
        else {
          map = map.get(clusterId) match {
            case Some(xs:ListBuffer[Point]) => map.updated(clusterId, point +: xs)
            case None => map
          }
        }
      }

      // convert map of key: clusterId, value: List[Points] -> Array[(Array[Point], Int)]
      val tempList: ListBuffer[(Array[Point], Int)] = ListBuffer()
      map.keys.foreach(clusterId => tempList += Tuple2(map(clusterId).toArray, clusterId))

      clusters2 = tempList.toArray
    }

    println("Running CURE...")

    start = System.currentTimeMillis()

    val cure = new Cure(clusters2, numClusters, shrinkFactor, numRepresentatives, ss)
    val finalClusters: RDD[Cluster] = cure.runWithoutMerge()

    end = System.currentTimeMillis()
    println("Total time (CURE): " + (end - start) + " ms")

    finalClusters.foreach(println)

    // CURE SILHOUETTE
    val evaluator = new ClusteringEvaluator().
      setPredictionCol("prediction").
      setFeaturesCol("features").
      setMetricName("silhouette")

    // calculate Silhouette score using representatives
    if (withRepresentatives){
      println("Calculate Silhouette score using representatives")

      val clustersToPoints: Array[(Point, Int)] = finalClusters
        .map(cluster => cluster.points.map{point: Point => (point, cluster.id)})
        .flatMap(l => l).collect()

      val bFinalClusters = ss.sparkContext.broadcast(clustersToPoints)

      val newPredictions = data
        .map(row => (row.getDouble(0), row.getDouble(1),
          Array(row.getDouble(0), row.getDouble(1)),
          this.closestCluster(row.getDouble(0),
            row.getDouble(1),
            bFinalClusters.value.map{case (point, id) => (point.dimensions, id)})))
        .toDF("x", "y", "features", "prediction")

      val silhouette = evaluator.evaluate(newPredictions)
      println("Silhouette with squared euclidean distance for CURE = " + silhouette)
    } // calculate Silhouette score using cluster centers
    else {
      println("Calculate Silhouette score using cluster centers")

      val clustersForEvaluation = finalClusters
        .map { cluster: Cluster => (this.meanPoint(cluster.points).dimensions, cluster.id) }
        .toDF("features", "prediction").as[(Array[Double], Int)].collect()

      val bFinalClusters = ss.sparkContext.broadcast(clustersForEvaluation)

      val newPredictions = data
        .map(row => (row.getDouble(0), row.getDouble(1),
          Array(row.getDouble(0), row.getDouble(1)),
          this.closestCluster(row.getDouble(0),
            row.getDouble(1),
            bFinalClusters.value)))
        .toDF("x", "y", "features", "prediction")

      val silhouette = evaluator.evaluate(newPredictions)
      println("Silhouette with squared euclidean distance for CURE = " + silhouette)
    }
  }

  /*
    Finds the cluster closer to the given point
   */
  def closestCluster(dim1: Double, dim2: Double, clusterPoints: Array[(Array[Double], Int)]): Int = {
    var minDistance: Double = Double.MaxValue
    var clusterId: Int = -1

    for (tuple <- clusterPoints){
      val center = tuple._1
      val distance = this.distanceFrom(center, Array(dim1, dim2))
      if (distance < minDistance){
        minDistance = distance
        clusterId = tuple._2
      }
    }

    clusterId
  }

  /*
   Calculates the distance between 2 points
   */
  def distanceFrom(dim1 : Array[Double], dim2 : Array[Double], distType: String = "square"): Double ={

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.pow(dimA - dimB, 2)}
    val distance : Double = distances.sum

    if (distType == "euclidean") Math.sqrt(distance)
    else distance
  }

  /*
 calculate mean of 2d points
  */
  def meanPoint(points: Array[Point]): Point = {
    val lengthOfPoints: Int = points.length
    var x: Double = 0
    var y: Double = 0
    for (point <- points){
      x += point.dimensions(0)
      y += point.dimensions(1)
    }

    x = x / lengthOfPoints
    y = y / lengthOfPoints
    val mean = Point(Array(x, y), -1)
    mean

  }

  /*
    Recursively delete directory or file
   */
  def delete(file: File): Unit = {
    if (file.isDirectory) {
      if (file.list.length == 0) {
        file.delete()
      }
      else {
        for (nestedFileName: String <- file.list()) {
          val nestedFile: File = new File(file, nestedFileName)
          delete(nestedFile)
        }
        if (file.list.length == 0) {
          file.delete()
        }
      }
    }
    else {
      file.delete()
    }
  }
}
