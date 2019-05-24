

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager
object Cure {

  var CLUSTERS: Broadcast[Int] = _
  var REPRESENTATIVES: Broadcast[Int] = _
  var SHRINK_FACTOR: Broadcast[Double] = _

  def run(data: DataFrame, k: Int, represenatives: Int, ss: SparkSession): Unit = {

    CLUSTERS = ss.sparkContext.broadcast(k)
    REPRESENTATIVES = ss.sparkContext.broadcast(represenatives)
    SHRINK_FACTOR = ss.sparkContext.broadcast(0.2)

    val idCounter = ss.sparkContext.longAccumulator
    val dataRdd = data.rdd

    // initially each point is a separate cluster
    val points : RDD[Point] = dataRdd.map(instance => {  // assign cluster id to each point
      val p =Point(Array(instance.getDouble(0), instance.getDouble(1)), cluster = null)
      p.cluster = Cluster(points = Array(p), id = idCounter.value, representatives = Array(p), closest = null, meanPoint = p)
      idCounter.add(1)
      p
    }).cache()

    val clusters: RDD[Cluster] = preCluster(points, 4)
    clusters.foreach(cluster => {
      println(cluster)
      println("Representatives")
      cluster.representatives.foreach(println)
      println("Points")
      cluster.points.foreach(println)
    })


  }

  /*
    Given a point list create a tree from the root of the list and iteratively insert points into the tree
   */
  def createTree(points: List[Point]) : KdTree = {
    val root = points.head

    val kdTree = new KdTree(Node(root, null, null), root.dimensions.length)
    for (i <- 1 until points.length - 1) {
      kdTree.insert(points(i))
    }
    kdTree
  }

  /*
    Given a point list and a Kd tree create a heap from the points and the calculated closest points and clusters
   */
  def createHeap(points: List[Point], kdTree: KdTree) : MinHeap = {

    val minHeap = new MinHeap(points.length)
    val log = LogManager.getRootLogger
    log.warn("Points: " + points.length)
    log.warn("Tree size: " + kdTree.getSize)
    log.warn("Heap size before: " + minHeap.getSize)
    points.map(point => {
      val closestPoint = kdTree.closestClusterPoint(point)
      point.cluster.closest = closestPoint.cluster // Assign the cluster of the closest point to the closest field of the point's cluster
      point.cluster.distanceFromClosest = Utils.squaredDistance(point, closestPoint)
      minHeap.insert(point.cluster)
      point.cluster
    })
    log.warn("Heap size after: " + minHeap.getSize)
    log.warn("++++++++++++")
    minHeap
  }

  /*
  calculate mean of 2d points
   */
  def meanPoint(points: Array[Point]): Point = {
    val lengthOfPoints: Int =points.length
    var x: Double = 0
    var y: Double = 0
    for (point <- points){
      x += point.dimensions(0)
      y += point.dimensions(1)
    }

    x = x / lengthOfPoints
    y = y / lengthOfPoints
    val mean = Point(Array(x, y), null)
    mean

  }

  /*
    Shrink representative array by SHRINKING FACTOR
   */
  def shrinkRepresentatives(shrinkFactor: Double, representatives: Array[Point], mean: Point) : Array[Point] = {

    // copy array so as to shrink only the copy and not the original
    val tmpArray = new Array[Point](representatives.length)
    tmpArray.indices.foreach(i => {
      if(representatives(i) == null)tmpArray(i) = null
      else tmpArray(i) = Point(representatives(i).dimensions.clone())
    })

    val representativesCopy: Array[Point] = tmpArray


    representativesCopy.foreach(representative => {
      if (representative != null) {
        val dimensions = representative.dimensions
        dimensions.indices.foreach(dim => dimensions(dim) = dimensions(dim) + shrinkFactor *(mean.dimensions(dim) - dimensions(dim)))
      }
    })

    representativesCopy
  }

  /*
   calculate the representatives of a cluster according to merge method of original paper (figure 6)
   */
  def calculateRepresentatives(numOfPoints: Int, points: Array[Point], mean: Point): Array[Point] = {
    val representatives = new Array[Point](numOfPoints)

    for (i <- 0 until numOfPoints) {
      var maxDist: Double = 0
      var minDist: Double = 0
      var maxPoint: Point = null

      points.foreach(p => {
        if (!representatives.contains(p)) {
          if (i == 0) {
            minDist = Utils.squaredDistance(p, mean)
          }
          else {
            // calculate minimum distance for all points in temp
            minDist = representatives.foldLeft(Double.MaxValue) { (currentDistance, q) => {
              if (q == null) {
                currentDistance
              }
              else {
                val distance = Utils.squaredDistance(p, q)
                if (distance < currentDistance) distance
                else currentDistance
              }
            }
            }
          }
          if (minDist >= maxDist) {
            maxDist = minDist
            maxPoint = p
          }
        }
      }
      )
      representatives(i) = maxPoint
    }

    representatives
  }

  /*
    Merge 2 clusters according to original paper's merge algorithm implementation (figure 6)
   */
  def merge(u: Cluster, v: Cluster, c: Int, shrinkFactor:Double) : Cluster = {

    val w: Array[Point] = u.points ++ v.points

    val mean: Point = meanPoint(w)

    val mergedCluster: Cluster = {
      if (w.length <= c) {
        Cluster(w, 0 , shrinkRepresentatives(shrinkFactor, w, mean), null, mean)
      }
      else {

        val representatives : Array[Point] = calculateRepresentatives(c, w, mean)

        val shrinked = shrinkRepresentatives(shrinkFactor, representatives, mean)
        val newCluster = Cluster(w, 0, shrinked, null, mean)
        newCluster
      }
    }

    mergedCluster.representatives.foreach(_.cluster = mergedCluster)
    mergedCluster.points.foreach(_.cluster = mergedCluster)
    mergedCluster.meanPoint.cluster = mergedCluster
    mergedCluster
  }


  // get the nearest cluster to point and the corresponding distance
  def getNearestCluster(representatives: Array[Point], kdTree: KdTree): (Cluster, Double) = {
    // for every point in the representatives find the closest
    // keep the total minimum distance and return the corresponding cluster
    var minDistance: Double = Double.MaxValue
    var closestCluster: Cluster = null
    for (p <- representatives){
      val closest: Point = kdTree.closestClusterPoint(p) // closest belongs to a different cluster than the representatives

      val currentDistance: Double = Utils.squaredDistance(p, closest)
      if ( currentDistance < minDistance){
        minDistance = currentDistance
        closestCluster = closest.cluster
      }
    }

    (closestCluster, minDistance)
  }

  def preCluster(points: RDD[Point], numOfPartitions: Int): RDD[Cluster] ={

    val repartPoints: RDD[Point] = points.repartition(numOfPartitions)

    val clusters: RDD[Cluster] = repartPoints.mapPartitions(partition => {
      val c = REPRESENTATIVES.value
      val shrinkFactor = SHRINK_FACTOR.value
      val k = CLUSTERS.value

      val data = partition.toList

      // if points are more than clusters, compute clusters else return a new cluster for each point
      if(data.lengthCompare(k) > 0) {
        // create the necessary structures
        val kdTree: KdTree = createTree(data)
        val minHeap: MinHeap = createHeap(data, kdTree)

        val log = LogManager.getRootLogger
        log.warn("NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN")
        // implementation of cluster algorithm of original paper (figure 5)
        while (minHeap.getSize > k) {
          val u: Cluster = minHeap.extractMin()
          val v: Cluster = u.closest

//          minHeap.delete(v)

          // merge u and v clusters into w
          val w = merge(u, v, c, shrinkFactor)

          // remove u and v representatives from kd tree
          u.representatives.foreach(kdTree.delete)
          v.representatives.foreach(kdTree.delete)

          // find the cluster closer to w
          val (closestCluster, closestDistance) = getNearestCluster(w.representatives, kdTree)
          w.closest = closestCluster
          w.distanceFromClosest = closestDistance

          // insert the w representatives into the kd tree
          w.representatives.foreach(kdTree.insert)
          removeClustersFromHeapUsingReps(kdTree, minHeap, u, v)
//          for (i <- 0 to minHeap.getSize){
//            val x: Cluster = minHeap.getMinHeap(i)
//
//            // if cluster closer to x is either u or v
//            if (x.closest == u || x.closest == v){
//              if (Utils.clusterDistance(x, x.closest) < Utils.clusterDistance(x, w)){
//                // any of the other clusters could become the new closest to x
//                val (xClosest, xDistance) = getNearestCluster(x.representatives, kdTree)
//                x.closest = xClosest
//                x.distanceFromClosest = xDistance
//              }
//              else {
//                // w is the closest to x, so assign it
//                x.closest = w
//                x.distanceFromClosest = Utils.clusterDistance(x, w)
//              }
//              minHeap.heapify(i)
//            }
//            else if (Utils.clusterDistance(x, x.closest) > Utils.clusterDistance(x, w)){
//              x.closest = w
//              x.distanceFromClosest = Utils.clusterDistance(x, w)
//              minHeap.heapify(i)
//            }
//          }

          minHeap.insert(w)
        }


        log.warn(minHeap.getSize)
        log.warn("===================")
        minHeap.getMinHeap.map(cluster => {
          cluster.points.foreach(_.cluster = null)
          val reps = cluster.representatives
          Cluster(calculateRepresentatives(c, cluster.points, cluster.meanPoint), 0, reps, null, cluster.meanPoint, cluster.distanceFromClosest)
        }).toIterator
      }
      else {
        data.map(point => {
          Cluster(Array(point), 0,  Array(point), null, point)
        }).toIterator
      }
    }
    )

    clusters
  }

  private def removeClustersFromHeapUsingReps(kdTree: KdTree, cHeap: MinHeap, c1: Cluster, nearest: Cluster): Unit = {
    val heapArray = cHeap.getMinHeap // now we need to delete the cluster points of c1 and nearest from Heap
    val heapSize = cHeap.getSize
    var it = 0
    while (it < heapSize) {
      var flag = false
      val tmpCluster = heapArray(it)
      val tmpNearest = tmpCluster.closest
      if (tmpCluster == nearest){
        cHeap.delete(it) //remove cluster
        flag = true
      }
      if (tmpNearest == nearest || tmpNearest == c1) { //Re Compute nearest cluster
        val (newCluster, newDistance) = getNearestCluster(tmpCluster.representatives, kdTree)
        tmpCluster.closest = newCluster
        tmpCluster.distanceFromClosest = newDistance
        cHeap.heapify(it)
        flag = true
      }
      if(!flag) it = it + 1
    }
  }
}
