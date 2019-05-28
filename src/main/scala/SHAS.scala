import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer
import scala.io.Source

/*
  K decides how many intermediate MSTs to be merged on each iteration to avoid computational load
 */
class SHAS(data: DataFrame, dims: Int = 2, splits: Int = 2, k: Int = 3, ss: SparkSession) extends Serializable {

  @transient val  log: Logger = LogManager.getRootLogger
  val numPoints: Int = data.count().toInt
  val numDimensions: Int = 2
  val numSplits: Int = 2
  val K: Int = k

  log.warn("Number of points: " + numPoints)
  log.warn("Dimensions: " + numDimensions)
  log.warn("Number of splits: " + numSplits)
  log.warn("Number of MST merges on each iteration: " + K)

  /*
    A dataset D is divided into two disjoint data sets D1 and D2, forming 3 sub graphs
    G(D1), G(D2), Gb(D1, D2) where Gb(D1, D2) is the complete bipartite graph on D1 and D2
    and G(D1), G(D2) the complete weighted graphs of D1 and D2 accordingly
    In this way, any possible edge is assigned to some sub graph
    and taking the union of these would return the original graph
   */
  def run(clusters: Int): Unit ={

//    var numGraphs: Int = numSplits * numSplits / 2
//    numGraphs = (numGraphs + (K-1)) / K
    var numGraphs: Int = numSplits * (numSplits - 1) / 2 + numSplits

    val fileCreator = new FileCreator(ss.sparkContext)
    fileCreator.createPartitionFiles(numGraphs = numGraphs)
    fileCreator.writeSequenceFiles(data.rdd, numPoints, numDimensions)

    val subGraphIdRDD : RDD[String] = ss.sparkContext.textFile("produced_data/subgraphIds", numGraphs)
    val start: Long = System.currentTimeMillis()

    val subMSTs: RDD[(Int, Edge)] = subGraphIdRDD.flatMap(id => localMST(id, "produced_data/dataPartitions"))

    var mstToBeMerged: RDD[(Int, Iterable[Edge])] = subMSTs.combineByKey((edge: Edge) => createCombiner(edge),
      (edges: Iterable[Edge], edge: Edge) => mergeValue(edges, edge),
      (edges1: Iterable[Edge], edges2: Iterable[Edge]) => kruskal(edges1, edges2),
      numGraphs).cache()

    while (numGraphs > 1){
      numGraphs = (numGraphs + (K-1)) / K

      mstToBeMerged = mstToBeMerged.map(mst => setPartitionIdFunction(mst, K))
        .reduceByKey((edges1: Iterable[Edge], edges2: Iterable[Edge]) => kruskal(edges1, edges2),
                    numGraphs)
    }

//    mstToBeMerged.collect().foreach(instance => println(instance._1 + ": " + instance._2.foreach(println)))

    val end: Long = System.currentTimeMillis()
    log.warn("Total time: " + (end - start))

    val c: Array[Array[Int]] = extractClusters(mstToBeMerged, clusters)

  }

  /*
    For k clusters, remove the first k cheapest edges. The arising connected components are the clusters
   */
  def extractClusters(mstToBeMerged: RDD[(Int, Iterable[Edge])], numClusters: Int): Array[Array[Int]] = {

    // if we want a single cluster, just return the nodes in the MST
    if (numClusters == 1) {
      val returned = new ListBuffer[Array[Int]]
      returned += mstToBeMerged.map(_._2.toArray).collect().flatten.map( (e: Edge) => e.left)

      return returned.toArray
    }

    var edges: Iterable[Edge] = mstToBeMerged.map(_._2).collect()(0) // we are sure there is only one MST, so collect each edges

    edges.foreach(println)

    var edgeArray = edges.toArray
    // separate edges to remove from the rest edges
    val removedEdges: ListBuffer[Edge] = new ListBuffer[Edge]
    for (i <- 0 until numClusters - 1){
      removedEdges += edgeArray(i)
    }

    // remove removedEdges from edgeList
    edgeArray = edgeArray.filter(edge => !(removedEdges contains edge))

    // for each edge node create a cluster containing all of its subnodes
    val clusters: ListBuffer[ListBuffer[Int]] = new ListBuffer[ListBuffer[Int]]

    // iterate over the removed edges
    for (edge <- removedEdges){
      // if end then we have completed the traversal of nodes

        val left: Int = edge.left
        val right: Int = edge.right

        println("Edge: " + left + " - " + right)

        if (clusters.isEmpty) {
          // find left node's subtree
          clusters += this.updateClusters(left, edgeArray)
          println("Left ended")
          // find right node's subtree
          clusters += this.updateClusters(right, edgeArray)
          println("Right ended")
        }// more than 2 clusters. we need to find the parent cluster of the divided ones and remove it from the list before inserting the 2 new clusters
        else{

        }
      }


    // convert listbuffer to array
    val returned = clusters.map(x => x.toArray)
    returned.foreach(arr => println(arr.foreach(el => print(el + " "))))
    returned.toArray
  }

  /*
    Add node to its cluster and traverse the MST top-down to insert children too
   */
  def updateClusters(node: Int, edges: Array[Edge]): ListBuffer[Int] = {

    var clusters = new ListBuffer[Int]

    val nodeEdges: Array[Edge] = edges.filter(edge => edge.left == node || edge.right == node)

    // add node to cluster
    clusters += node

    // if there are edges with children traverse
    if (nodeEdges.length > 0 ) {
      // for each node's edge take the node on the other side of this edge
      for (j <- nodeEdges.indices) {
        val otherNode: Int = if (nodeEdges(j).left == node) nodeEdges(j).right else nodeEdges(j).left

        // remove the current edge from edge list
        val newEdges: Array[Edge] = edges.filter(edge => edge != nodeEdges(j))
        // recurse for other node
        clusters ++= updateClusters(otherNode, newEdges)
      }
    }

    println("Cluster of " + node)
    clusters.foreach(println)
    clusters
  }

  /*
    The merge combiner method of the combineByKey transformation
    Follows Kruskal's logic -> insert cheapest edge as long as it doesn't form a cycle
    Uses Union-Find structure to efficiently combine MST's and detect cycles
    Most neighboring sub graphs share half of the points -> Combining K consecutive graphs leads to many overlapping edges
      Eliminate incorrect edges early and reduce communication cost
   */
  def kruskal(edges1: Iterable[Edge], edges2: Iterable[Edge]): Iterable[Edge] = {

    val unionFind: UnionFind = new UnionFind(numPoints)
    val edges: ListBuffer[Edge] = new ListBuffer[Edge]()

    val numEdges = numPoints - 1

    val leftEdges: Iterator[Edge] = edges1.iterator
    val rightEdges: Iterator[Edge] = edges2.iterator

    var leftEdge: Edge = leftEdges.next()
    var rightEdge: Edge = rightEdges.next()

    // track the edge with the minimum weight
    var minEdges: Iterator[Edge] = null
    var minEdge: Edge = null
    var isLeft: Boolean = false

    do {

      //assign minimum to left or right
      minEdges = if (Math.min(leftEdge.weight, rightEdge.weight) == leftEdge.weight) leftEdges else rightEdges
      minEdge = if (Math.min(leftEdge.weight, rightEdge.weight) == leftEdge.weight) leftEdge else rightEdge
      isLeft = if (minEdge == leftEdge) true else false

      // add minimum edge to returned edges if it does not form a cycle
      if (unionFind.union(minEdge.left, minEdge.right)) {
        edges += minEdge
      }

      // assign new minimum edge
      minEdge = if (minEdges.hasNext) minEdges.next() else null
      if (isLeft) leftEdge = minEdge
      else rightEdge = minEdge

    } while (minEdge != null && edges.length < numEdges)

    //continue with the remaining edges
    minEdges = if (isLeft) rightEdges else leftEdges
    minEdge = if (isLeft) rightEdge else leftEdge

    while (minEdge != null && edges.length < numEdges) {
      if (unionFind.union(minEdge.left, minEdge.right)) {
        edges += minEdge
      }
      minEdge = if (minEdges.hasNext) minEdges.next() else null
    }

    edges
  }
  /*
    The create combiner method of the combineByKey transformation
   */
  def createCombiner(edge: Edge): Iterable[Edge] = {
    val edges: ListBuffer[Edge] = new ListBuffer[Edge]()
    edges += edge
    edges
  }

  /*
    The merge method for every key of combineByKey transformation
   */
  def mergeValue(edges: Iterable[Edge], edge: Edge): Iterable[Edge] = {
    val totalEdges: ListBuffer[Edge] = edges.to[ListBuffer]
    totalEdges += edge
    totalEdges

  }

  def setPartitionIdFunction(mst: (Int, Iterable[Edge]), K: Int): (Int, Iterable[Edge]) = {
    (mst._1 / K, mst._2)
  }

  /*
    Creates an MST from the subgraphs of the given id and returns the edges of the tree as an array
   */
  def localMST(row: String, fileLocation: String): Array[(Int, Edge)] = {
    val id: Int = row.toInt

    val pair: (Array[Point], Array[Point]) = this.getPair(id, fileLocation)

    val mst: MST = new MST(pair, id)
    val edgeList: Array[(Int, Edge)] = mst.getEdges

    edgeList
  }

  /*
  get pair of lists of points
   */
  def getPair(id: Int, fileLocation: String): (Array[Point], Array[Point]) = {

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
