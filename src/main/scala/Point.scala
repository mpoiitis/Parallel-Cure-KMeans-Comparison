case class Point(var dimensions: Array[Double] = null, var id: Int = 0) {

  def this(point: Point){
    this(point.dimensions.clone(), point.id)
  }

  def getDimension:Int ={
    this.dimensions.length
  }

  def distanceFrom(point: Point, distType: String = "square"): Double ={

    val dim1 : Array[Double] = this.dimensions
    val dim2 : Array[Double] = point.dimensions

    val distances : Array[Double] = (dim1 zip dim2).map{case (dimA, dimB) => Math.pow(dimA - dimB, 2)}
    val distance : Double = distances.sum

    if (distType == "euclidean") Math.sqrt(distance)
    else distance
  }


  def canEqual(a: Any): Boolean = a.isInstanceOf[Point]

  override def equals(that: Any): Boolean = that match {
    case that: Point => that.canEqual(this) && this.dimensions.deep == that.dimensions.deep && this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = {
    var sum: Double = 0
    this.dimensions.foreach(sum += _)
    this.id * 157 + this.dimensions.length * 43 + sum.toInt
  }

  override def toString: String = this.id + " " + this.dimensions.mkString(",")
}
