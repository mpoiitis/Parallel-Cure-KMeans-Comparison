

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.log4j.LogManager
import org.apache.spark.util.LongAccumulator
object Cure {

  var CLUSTERS: Broadcast[Int] = _
  var REPRESENTATIVES: Broadcast[Int] = _
  var SHRINK_FACTOR: Broadcast[Double] = _
  var CLUSTER_ID: LongAccumulator = _

  def run(data: DataFrame, k: Int, representatives: Int, ss: SparkSession): Unit = {

    CLUSTERS = ss.sparkContext.broadcast(k)
    REPRESENTATIVES = ss.sparkContext.broadcast(representatives)
    SHRINK_FACTOR = ss.sparkContext.broadcast(0.2)

    CLUSTER_ID = ss.sparkContext.longAccumulator
    val dataRdd = data.rdd

    // initially each point is a separate cluster
    val points : RDD[Point] = dataRdd.map(instance => {  // assign cluster id to each point
      val p =Point(Array(instance.getDouble(0), instance.getDouble(1)), cluster = null)
      p.cluster = Cluster(points = Array(p), id = CLUSTER_ID.value, representatives = Array(p), closest = null, meanPoint = p)
      CLUSTER_ID.add(1)
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
    for (i <- 1 until points.length) {
      kdTree.insert(points(i))
    }
    kdTree
  }

  /*
    Given a point list and a Kd tree create a heap from the points and the calculated closest points and clusters
   */
  def createHeap(points: List[Point], kdTree: KdTree) : MinHeap = {

    val minHeap = new MinHeap(points.length)

    points.map(point => {
      val closestPoint = kdTree.closestClusterPoint(point)
      point.cluster.closest = closestPoint.cluster // Assign the cluster of the closest point to the closest field of the point's cluster
      point.cluster.distanceFromClosest = Utils.squaredDistance(point, closestPoint)
      minHeap.insert(point.cluster)
      point.cluster
    })

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

    val mean: Point = this.meanPoint(w)

    val mergedCluster: Cluster = {
      var shrinked: Array[Point] = null

      // if cluster points are more than number of representatives, calculate representatives
      if (w.length <= c) {
        shrinked = shrinkRepresentatives(shrinkFactor, w, mean)
      }
      else {
        val representatives : Array[Point] = calculateRepresentatives(c, w, mean)
        shrinked = shrinkRepresentatives(shrinkFactor, representatives, mean)
      }

      val newCluster = Cluster(w, CLUSTER_ID.value, shrinked, null, mean)
      CLUSTER_ID.add(1)
      newCluster
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

        var j = 0
        val log = LogManager.getRootLogger
        // implementation of cluster algorithm of original paper (figure 5)
        while (minHeap.getSize > k) {

          val u: Cluster = minHeap.extractMin()
          val v: Cluster = u.closest

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

          var i = 0

          while (i < minHeap.getSize) {
            var flag = false
            val x = minHeap.getMinHeap(i)
            if (x == v){
              minHeap.delete(i) //remove cluster
              flag = true
            }
            // if cluster closer to x is either u or v
            if (x.closest == u || x.closest == v) {

              if (Utils.clusterDistance(x, x.closest) < Utils.clusterDistance(x, w)) {
                // any of the other clusters could become the new closest to x
                val (xClosest, xDistance) = getNearestCluster(x.representatives, kdTree)
                x.closest = xClosest
                x.distanceFromClosest = xDistance
              }
              else{
                // w is the closest to x, so assign it
                x.closest = w
                x.distanceFromClosest = Utils.clusterDistance(x, w)
              }
              minHeap.heapify(i)
              flag = true
            }
            else if (Utils.clusterDistance(x, x.closest) > Utils.clusterDistance(x, w)){
              x.closest = w
              x.distanceFromClosest = Utils.clusterDistance(x, w)
              minHeap.heapify(i)
              flag = true
            }
            // if no minHeap relocation happened proceed in heap
            if(!flag) {
              i = i + 1
            }

          }

          minHeap.insert(w)
          log.warn("j: " +j)
          j += 1
        }


        if (j % 100 == 0){
          log.warn(j)
        }
        minHeap.getMinHeap.map(cluster => {
          log.warn("Before")
          cluster.points.foreach(_.cluster = null)
          log.warn("After")
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

}
