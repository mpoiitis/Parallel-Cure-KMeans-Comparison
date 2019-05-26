import java.io.File

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class FileCreator(sc: SparkContext, numSplits: Int = 2) {

  val log: Logger = LogManager.getRootLogger

  def createPartitionFiles(fileLocation: String = "produced_data/subgraphIds", numGraphs: Int): Unit = {



    val file = new File(fileLocation)
    if (file.exists()){
      log.warn("Directory exists, deleting...")
      this.delete(file)
    }

    log.warn("Num of graphs: " + numGraphs)

    val subgraphIds: Array[String] = new Array[String](numGraphs)

    for (i <- 0 until numGraphs){
      subgraphIds(i) =  String.valueOf(i)
    }

    log.warn("create subgraphIds files: " + fileLocation)
    sc.parallelize(subgraphIds, numGraphs).saveAsTextFile(fileLocation)

  }

  def writeSequenceFiles(data: RDD[Row], numPoints: Int, numDimensions: Int, dataParτitionFilesLocation: String = "produced_data/dataPartitions"): Unit ={

    log.warn("Num of partitions: " + numSplits)
    val points : RDD[Point] = data.zipWithIndex.map{ case (row, i) =>
      val dimensions = Array(row.getDouble(0), row.getDouble(1))
      Point(dimensions, i)
    }

    val file = new File(dataParτitionFilesLocation)
    if (file.exists()) {
      log.warn("Directory exists, deleting...")
      this.delete(file)
    }

    log.warn("create dataPartitions files: " + dataParτitionFilesLocation)
    points.repartition(numSplits).saveAsTextFile(dataParτitionFilesLocation)

  }

  /*
    Recursively delete directory or file
   */
  def delete(file: File): Unit ={
    if(file.isDirectory){
      if (file.list.length == 0){
        file.delete()
      }
      else {
        for (nestedFileName: String <- file.list()){
          val nestedFile: File = new File(file, nestedFileName)
          delete(nestedFile)
        }
        if (file.list.length == 0){
          file.delete()
        }
      }
    }
    else{
      file.delete()
    }

  }
}
