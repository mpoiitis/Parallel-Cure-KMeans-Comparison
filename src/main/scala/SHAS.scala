import org.apache.spark.sql.{DataFrame, SparkSession}

class SHAS(data: DataFrame, ss: SparkSession) {

  def run(): Unit ={

    val numPoints: Int = data.count().toInt
    val numDimensions: Int = 2
    val K: Int = 3 // decides how many intermediate MSTs to be merged on each iteration to avoid computational load
    val numSplits: Int = 2

//    var numGraphs: Int = numSplits * numSplits / 2
//    numGraphs = (numGraphs + (K-1)) / K
    val numGraphs: Int = numSplits * (numSplits - 1) / 2 + numSplits

    val fileCreator = new FileCreator(ss.sparkContext)
    fileCreator.createPartitionFiles(numGraphs = numGraphs)
    fileCreator.writeSequenceFiles(data.rdd, numPoints, numDimensions)
  }
}
