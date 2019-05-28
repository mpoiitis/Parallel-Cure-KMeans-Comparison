import org.apache.log4j.{LogManager, Logger}

import scala.collection.mutable.ListBuffer
import util.control.Breaks._

class MST(pair: (Array[Point], Array[Point]), id: Int){
  val left: Array[Point] = pair._1
  val right: Array[Point] = pair._2
  val isBipartite: Boolean = if(right == null) false else true
  val log: Logger = LogManager.getRootLogger

//  if(isBipartite) log.warn("Bipartite Sub-MST") else log.warn("Not Bipartite Sub-MST")

  /*
    return the tree's edges as array
   */
  def getEdges: Array[(Int, Edge)] = {

    val edgeList: Array[(Int, Edge)]= {
      if (this.isBipartite) this.bipartiteMST()
      else this.prim()
    }

    edgeList
  }

  def prim(): Array[(Int, Edge)] = {
    val data: Array[Point] = this.left
    val numPoints: Int = data.length

    var edges: ListBuffer[(Int, Edge)] = new ListBuffer[(Int, Edge)]()

    val left: Array[Int] = new Array[Int](numPoints)
    val parent: Array[Int] = new Array[Int](numPoints)
    val distances: Array[Double] = new Array[Double](numPoints)

    for (i <- 0 until numPoints){
      left(i) = i+1
      distances(i) = Double.MaxValue
    }

    distances(0) = 0
    parent(0) = -1

    var currentPoint: Int = 0
    var otherPoint: Int = 0
    var next: Int = 0
    var shift = 0

    for (i <- (numPoints - 1) to 0 by -1){

      shift = 0
      currentPoint = next
      next = left(shift)

      // find the cheapest edge for the right split
      var minimum: Double = Double.MaxValue
      for (j <- 0 until i){
        otherPoint = left(j)
        val distance = data(currentPoint).distanceFrom(data(otherPoint))
        if (distances(otherPoint) > distance){
          distances(otherPoint) = distance
          parent(otherPoint) = currentPoint
        }

        if (distances(otherPoint) < minimum) {
          minimum = distances(otherPoint)
          next = otherPoint
          shift = i
        }
      }

      val globalNext: Int = data(next).id
      val globalNextParent: Int = data(parent(next)).id

      // append in edges
      val edge: Edge = Edge(Math.min(globalNext, globalNextParent), Math.max(globalNext, globalNextParent), minimum)
      edges += Tuple2(this.id, edge)

      if (!(i-1 < 0)){
        left(shift) = left(i-1)
      }

    }

    edges = edges.sortWith(sortByEdgeWeight)
    log.warn("Not bipartite num of edges: " + edges.toArray.length)
    edges.toArray
  }

  /*
    An edge weight array for each of the 2 splits
    Select vertex v0 in the left split
    Populate array with every edge from v0 to every vertex in the right split, record cheapest edge (v0, vt)
    Populate array with every edge from vt to every vertex in the left split (except v0), record cheapest edge (vt, vt')
    The cheapest edge of both is picked. The endpoint is the next iteration point
   */
  def bipartiteMST(): Array[(Int, Edge)] = {
      val leftSize: Int = this.left.length
      val rightSize: Int = this.right.length
      var leftTracking: Int = 0
      var rightTracking: Int = 0

      var edges: ListBuffer[(Int, Edge)] = new ListBuffer[(Int, Edge)]()

      // instantiate utility arrays for left subtree
      val nextL: Array[Int] = new Array[Int](leftSize)
      val parentL: Array[Int] = new Array[Int](leftSize)
      val distanceL: Array[Double] = new Array[Double](leftSize)
      for (i <- 0 until leftSize){
        nextL(i) = i
        parentL(i) = -1
        distanceL(i) = Double.MaxValue
      }

      // instantiate utility arrays for right subtree
      val nextR: Array[Int] = new Array[Int](rightSize)
      val parentR: Array[Int] = new Array[Int](rightSize)
      val distanceR: Array[Double] = new Array[Double](rightSize)
      for (i <- 0 until rightSize){
        nextR(i) = i
        parentR(i) = -1
        distanceR(i) = Double.MaxValue
      }


      // ensure that the structures referring to right subtree contain the most elements
      leftTracking = if (leftSize <= rightSize) leftSize else rightSize
      rightTracking = if (leftSize <= rightSize) rightSize else leftSize

      // contain indexes of points in each array
      var nextLeft: Array[Int] = if (leftSize <= rightSize) nextL else nextR
      var nextRight: Array[Int] = if (leftSize <= rightSize) nextR else nextL

      var parentLeft: Array[Int] = if (leftSize <= rightSize) parentL else parentR
      var parentRight: Array[Int] = if (leftSize <= rightSize) parentR else parentL

      var distanceLeft: Array[Double] = if (leftSize <= rightSize) distanceL else distanceR
      var distanceRight: Array[Double] = if (leftSize <= rightSize) distanceR else distanceL

      // contain the points in each array
      var localLeft: Array[Point] = if (leftSize <= rightSize) this.left else this.right
      var localRight: Array[Point] = if (leftSize <= rightSize) this.right else this.left


      parentLeft(0) = -1
      var currentPoint: Int = 0
      var next: Int = 0
      var otherPoint: Int = 0
      var switch: Boolean = true
      var shift = 0

      while (rightTracking > 0) {

          shift = 0
          currentPoint = next
          next = nextRight(shift)

          // find the cheapest edge for the right split
          var minimum: Double = Double.MaxValue
          for (i <- 0 until rightTracking) {
            switch = true
            otherPoint = nextRight(i)

            val distance: Double = localLeft(currentPoint).distanceFrom(localRight(otherPoint))
            // found a smaller distance, update the corresponding values
            if (distanceRight(otherPoint) > distance) {
              distanceRight(otherPoint) = distance
              parentRight(otherPoint) = currentPoint
            }
            if (distanceLeft(currentPoint) > distance) {
              distanceLeft(currentPoint) = distance
              parentLeft(currentPoint) = otherPoint
            }

            // found point with edge weight smaller than the minimum, so examine this one next
            if (distanceRight(otherPoint) < minimum) {
              minimum = distanceRight(otherPoint)
              next = otherPoint
              shift = i
            }
          }

          // find minimum of both splits
          var globalNext: Int = localRight(next).id
          var globalNextParent: Int = localLeft(parentRight(next)).id

          for (i <- 0 until leftTracking) {
            currentPoint = nextLeft(i)
            if (distanceLeft(currentPoint) < minimum) {
              minimum = distanceLeft(currentPoint)
              next = currentPoint
              shift = i

              otherPoint = parentLeft(currentPoint)
              switch = false
              globalNextParent = localLeft(currentPoint).id
              globalNext = localRight(otherPoint).id
            }
          }

          for (i <- 0 until leftTracking) {
            currentPoint = nextLeft(i)
            otherPoint = parentLeft(currentPoint)
          }
          if (leftTracking == leftSize && rightTracking == rightSize) {
            leftTracking -= 1
            nextLeft(0) = nextLeft(leftTracking)
          }

          // append in edges
          val edge: Edge = Edge(Math.min(globalNext, globalNextParent), Math.max(globalNext, globalNextParent), minimum)
          edges += Tuple2(this.id, edge)

          // the next elements comes from the left split
          if (!switch) {
            leftTracking -= 1
            nextLeft(shift) = nextLeft(leftTracking)
          }// the next elements comes from the right split
          else {
            rightTracking -= 1
            nextRight(shift) = nextRight(rightTracking)

            //swap left and right locals, parent, next, distance, number of points
            val tempLocal = localRight
            localRight = localLeft
            localLeft = tempLocal

            val tempParent = parentRight
            parentRight = parentLeft
            parentLeft = tempParent

            val tempNext = nextRight
            nextRight = nextLeft
            nextLeft = tempNext

            val tempDistance = distanceRight
            distanceRight = distanceLeft
            distanceLeft = tempDistance

            val numOfPoints = rightTracking
            rightTracking = leftTracking
            leftTracking = numOfPoints
          }
      }

      for (i <- 0 until leftTracking) {
        currentPoint = nextLeft(i)
        otherPoint = parentLeft(currentPoint)

        val minimum: Double = distanceLeft(currentPoint)
        val globalNext: Int = localRight(otherPoint).id
        val globalNextParent: Int = localLeft(currentPoint).id

        val edge: Edge = Edge(Math.min(globalNext, globalNextParent), Math.max(globalNext, globalNextParent), minimum)
        edges += Tuple2(this.id, edge)
      }

      edges = edges.sortWith(sortByEdgeWeight)
      log.warn("Bipartite num of edges: " + edges.toArray.length)
      edges.toArray
  }

  def sortByEdgeWeight(e1: (Int, Edge), e2: (Int, Edge)): Boolean = {
    val edge1: Edge = e1._2
    val edge2: Edge = e2._2

    edge1.weight < edge2.weight
  }
}
