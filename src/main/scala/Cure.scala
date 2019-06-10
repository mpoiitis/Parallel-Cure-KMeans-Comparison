import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions.min

class Cure(c: Array[(Array[Point], Int)], numClusters: Int, shrinkFactor: Double, numRepresentatives: Int, ss: SparkSession) extends Serializable {

  val clusterRdd: RDD[(Array[Point], Int)] = ss.sparkContext.parallelize(c)
  import ss.implicits._

  def run(): RDD[Cluster] ={

    var clusters = this.calculateRepresentatives().cache()
    while (clusters.count() > this.numClusters) {
      // find distances between any pair of clusters and remove distances = 0, as they refer to the same cluster
      val combinations: Dataset[(Cluster, Cluster, Double)] = clusters.cartesian(clusters)
        .map { case (c1, c2) => (c1, c2, this.clusterDistance(c1, c2)) }
        .filter { case (_, _, dist) => dist != 0.0 }.toDF().as[(Cluster, Cluster, Double)]
        .toDF().as[(Cluster, Cluster, Double)]

      // find minimum distance
      val minVal = combinations.agg(min("_3")) //.select("*").as[(Cluster, Cluster, Double)]

      // extract the cluster pair with the minimum distance
      // 2 pairs will be present, but they are the same pair with their respective clusters interchanged
      val clusterToMerge = combinations.join(minVal, $"_3" === $"min(_3)", joinType = "inner").select($"_1", $"_2").as[(Cluster, Cluster)].take(1)(0)

      // create the new merged cluster and transform it to RDD
      val newCluster: Cluster = this.merge(clusterToMerge._1, clusterToMerge._2)
      val newClusterRdd = ss.sparkContext.parallelize(Array(newCluster))

      // remove merged clusters from tracked clusters
      val mergedClustersRdd = ss.sparkContext.parallelize(Array(clusterToMerge._1, clusterToMerge._2))

      clusters = clusters.subtract(mergedClustersRdd)

      // append the merged cluster to tracked clusters
      clusters = clusters.union(newClusterRdd)
    }

    clusters
  }
  /*
   Calculate the representatives for all the clusters
   */
  def calculateRepresentatives(): RDD[Cluster] = {
    val bNumReps = this.ss.sparkContext.broadcast(this.numRepresentatives)

    val representatives: RDD[Cluster] = this.clusterRdd
      .map{ case (points, id) => Cluster(this.calculateRepresentativesForCluster(points, bNumReps.value), id)}

    representatives
  }

  /*
   Calculate the representatives for a given cluster
   */
  def calculateRepresentativesForCluster(points: Array[Point], numReps: Int): Array[Point] = {

    val mean: Point = this.meanPoint(points)

    var representatives = new Array[Point](numReps)

    for (i <- 0 until numReps) {
      var maxDist: Double = 0
      var minDist: Double = 0
      var maxPoint: Point = null

      //      val log = LogManager.getRootLogger

      points.foreach(p => {
        if (!representatives.contains(p)) {
          if (i == 0) {
            minDist = p.distanceFrom(mean)
          }
          else {
            // calculate minimum distance for all points in temp
            minDist = representatives.foldLeft(Double.MaxValue) { (currentDistance, q) => {
              if (q == null) {
                currentDistance
              }
              else {
                val distance = p.distanceFrom(q)
                if (distance < currentDistance) distance
                else currentDistance
              }
            }}
          }
          if (minDist >= maxDist) {
            maxDist = minDist
            maxPoint = p
          }
        }
      } )

      representatives(i) = maxPoint
    }

    // shrink representatives towards the center of the cluster
    representatives = representatives.filter(_!=null)
    representatives = this.shrinkRepresentatives(representatives, mean)
    representatives
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
    Shrink representative array towards the center of the cluster by the shrinking factor
   */
  def shrinkRepresentatives(representatives: Array[Point], mean: Point) : Array[Point] = {

    // copy array so as to shrink only the copy and not the original
    val temp = new Array[Point](representatives.length)
    temp.indices.foreach(i => {
      if(representatives(i) == null){
        temp(i) = null
      }
      else {
        temp(i) = Point(representatives(i).dimensions.clone())
      }
    })

    val representativesCopy: Array[Point] = temp


    representativesCopy.foreach(representative => {
      if (representative != null) {
        val dimensions = representative.dimensions
        dimensions.indices.foreach(dim => dimensions(dim) = dimensions(dim) + this.shrinkFactor *(mean.dimensions(dim) - dimensions(dim)))
      }
    })

    representativesCopy
  }

  /*
    Merge 2 clusters according to original paper's merge algorithm implementation (figure 6)
   */
  def merge(u: Cluster, v: Cluster) : Cluster = {

    val w: Array[Point] = u.points ++ v.points

    val mean: Point = this.meanPoint(w)

    var shrunk: Array[Point] = null
    // if cluster points are more than number of representatives, calculate representatives
    if (w.length <= this.numRepresentatives) {
      shrunk = shrinkRepresentatives(w, mean)
    }
    else {
      val representatives : Array[Point] = calculateRepresentativesForCluster(w, this.numRepresentatives)
      shrunk = shrinkRepresentatives(representatives, mean)
    }
    val minId = if (u.id < v.id) u.id else v.id
    val mergedCluster = Cluster(shrunk, minId)

    mergedCluster
  }

  /*
    Returns the distance between 2 clusters according to original paper's definition
    where we keep the smallest distance between any 2 points of different clusters
   */
  def clusterDistance(cluster1: Cluster, cluster2: Cluster): Double ={
    val crossProduct = cluster1.points.flatMap(p1 => cluster2.points.map(p2 => (p1, p2)))
    val differences = crossProduct.map{case (p1, p2) => p1.distanceFrom(p2)}
    val minDifference = differences.min

    minDifference
  }
}
