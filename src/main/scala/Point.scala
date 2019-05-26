case class Point(var dimensions: Array[Double] = null, var id: Int = 0) {

  def this(point: Point){
    this(point.dimensions.clone(), point.id)
  }

  def getDimension:Int ={
    this.dimensions.length
  }

  def distanceFrom(point: Point, distType: String = "square"): Double ={
    if (this.id == point.id){
      return 0
    }

    val dim1 : Array[Double] = this.dimensions
    val dim2 : Array[Double] = point.dimensions

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.pow(dimA - dimB, 2)}
    val distance : Double = distances.sum

    if (distType == "euclidean") Math.sqrt(distance)
    else distance
  }

  def equals(point: Point): Boolean ={
    if (this.id != point.id) return false

    val dim1 : Array[Double] = this.dimensions
    val dim2 : Array[Double] = point.dimensions
    val distWiseEquality : Array[Boolean] = (dim1 zip dim2).map{case (dimA, dimB) => dimA == dimB}.filter( condition => !condition)
    !(distWiseEquality.length > 0)
  }

  override def toString: String = this.id + " " + this.dimensions.mkString(",")
}
