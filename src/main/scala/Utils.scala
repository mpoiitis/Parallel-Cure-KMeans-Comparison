object Utils {

  /*
    Returns the euclidean distance between 2 points
   */
  def euclideanDistance(p1: Point, p2: Point): Double = {
    val dim1 : Array[Double] = p1.dimensions
    val dim2 : Array[Double] = p2.dimensions

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.pow(dimA - dimB, 2)}
    val distance : Double = distances.sum
    val squaredDistance : Double = Math.sqrt(distance)

    squaredDistance
  }

  /*
    Returns the squared distance between 2 points
    Avoids square root to save computation time according to https://en.wikipedia.org/wiki/K-d_tree#Nearest_neighbour_search
   */
  def squaredDistance(p1: Point, p2: Point): Double = {
    val dim1 : Array[Double] = p1.dimensions
    val dim2 : Array[Double] = p2.dimensions

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.pow(dimA - dimB, 2)}
    val distance : Double = distances.sum

    distance
  }

  /*
    Returns the manhattan distance between 2 points
   */
  def manhattanDistance(p1: Point, p2: Point): Double = {
    val dim1 : Array[Double] = p1.dimensions
    val dim2 : Array[Double] = p2.dimensions

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.abs(dimA - dimB)}
    val distance : Double = distances.sum

    distance
  }

  /*
    Returns true if the two points are equal, else false
   */
  def equals(p1: Point, p2: Point): Boolean = {
    val dim1 : Array[Double] = p1.dimensions
    val dim2 : Array[Double] = p2.dimensions

    val distWiseEquality : Array[Boolean] = (dim1 zip dim2).map{case (dimA, dimB) => dimA == dimB}.filter( condition => !condition)
    !(distWiseEquality.length > 0)
  }


  /*
    Returns the distance between 2 clusters according to original paper's definition
   */
  def clusterDistance(cluster1: Cluster, cluster2: Cluster): Double ={
    val crossProduct = cluster1.points.flatMap(p1 => cluster2.points.map(p2 => (p1, p2)))
    val differences = crossProduct.map{case (p1, p2) => squaredDistance(p1, p2)}
    val minDifference =differences.min

    minDifference
  }

}
