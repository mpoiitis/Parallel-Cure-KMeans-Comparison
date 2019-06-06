/**
  * Created by Zaikis Dimitrios, 8 and Poiitis Marinos, 17 on 13/05/2019.
  */


import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.min

object Main {

  def main(args: Array[String]): Unit = {

    if (args.length != 5) {
      println("Parameters needed! Parameters are: number_of_clusters, sample_size (percentage), num_intermediate_clusters, num_representatives, shrink_factor")
    }

    val ss = SparkSession.builder().master("local[*]").appName("BigDataApp").getOrCreate()
    Logger.getRootLogger.setLevel(Level.WARN)
    import ss.implicits._


//    val files = new java.io.File("data/").listFiles.filter(_.getName.endsWith(".txt"))
//
//    var data: DataFrame = null
//    for (file <- files) {
//      val fileDf= ss.read.csv(file.toString)
//      if (data!= null) {
//        data= data.union(fileDf)
//      } else {
//        data= fileDf
//      }
//    }
//
//    val numOfExamples = data.count()
//    val ratio = 0.01
//    data = data.sample(ratio)
//
//    println(numOfExamples)
//    println(data.count())

    // Cluster sample points hierarchically and in a parallel fashion using SHAS
    val data: DataFrame = ss.read.option("inferSchema","true").csv("data/data1.txt").toDF("x", "y").sample(args(1).toDouble)
    val shas = new SHAS(data, splits = 4, ss = ss)
    val clusters: Array[(Array[Point], Int)] = shas.run(numClusters = args(2).toInt)
    val cure = new Cure(clusters, args(0).toInt, args(4).toDouble, args(3).toInt, ss)
    val finalClusters: RDD[Cluster] = cure.run()

    finalClusters.foreach(println)
//    Thread.sleep(30000000)
    ss.stop()
  }




}
