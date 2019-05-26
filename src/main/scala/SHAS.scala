import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.Source

class SHAS(data: DataFrame, ss: SparkSession) extends Serializable {

  @transient val  log: Logger = LogManager.getRootLogger

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

    val subGraphIdRDD : RDD[String] = ss.sparkContext.textFile("produced_data/subgraphIds", numGraphs)
    val start: Long = System.currentTimeMillis()

    val subMSTs: RDD[(Int, Edge)] = subGraphIdRDD.flatMap(id => localMST(id, "produced_data/dataPartitions", numSplits))

    val end: Long = System.currentTimeMillis()

    log.warn("Total time: " + (end - start))
    log.warn(subMSTs.count())
  }

  /*
    Creates an MST from the subgraphs of the given id and returns the edges of the tree as an array
   */
  def localMST(row: String, fileLocation: String, numSplits: Int): Array[(Int, Edge)] = {
    val id: Int = row.toInt

    val pair: (Array[Point], Array[Point]) = this.getPair(id, numSplits, fileLocation)

    val mst: MST = new MST(pair, id)
    val edgeList: Array[(Int, Edge)] = mst.getEdges

    edgeList
  }

  /*
  get pair of lists of points
   */
  def getPair(id: Int, numSplits: Int, fileLocation: String): (Array[Point], Array[Point]) = {

    val numBipartite: Int = numSplits * (numSplits - 1) / 2

    // find which file to load
    var right: Int = -1
    var left: Int = -1

    if(id < numBipartite){
      right = this.getRight(id)
      left = this.getLeft(id, right)
    }
    else {
      left = id - numBipartite
      right = left
    }

    // actual filenames
    val leftFile: String = fileLocation + "/part-0000" + left
    val rightFile: String = fileLocation + "/part-0000" + right

    // get points from left file
    val leftPoints: Array[Point] = Source.fromFile(leftFile).getLines().toArray.map(stringPoint => {
      val firstSplit: Array[String] = stringPoint.split(" ")
      val id: Int = firstSplit(0).toInt
      val dimensions: Array[Double] = firstSplit(1).split(",").map(_.toDouble)
      Point(dimensions, id)
    })

    // get points from right file
    var rightPoints: Array[Point] = null

    if (!leftFile.equals(rightFile)){
      rightPoints = Source.fromFile(rightFile).getLines().toArray.map(stringPoint => {
        val firstSplit: Array[String] = stringPoint.split(" ")
        val id: Int = firstSplit(0).toInt
        val dimensions: Array[Double] = firstSplit(1).split(",").map(_.toDouble)
        Point(dimensions, id)
      })
    }

    (leftPoints, rightPoints)

  }

  /*
   Get right subfile
   Shifting left by n bits is equal to multiplying by 2^n
   Shifting right by n bits is equal to dividing by 2^n
   */
  def getRight(id: Int): Int = {
    //(Math.sqrt((id * Math.pow(2,3)) + 1).toInt + 1) / 2
    (Math.sqrt((id << 3) + 1).toInt + 1) >> 1
  }

  /*
    Get left subfile
   */
  def getLeft(id: Int, right: Int): Int = {
    //id - (((right - 1) * right) / 2)
    id - (((right - 1) * right) >> 1)
  }
}
