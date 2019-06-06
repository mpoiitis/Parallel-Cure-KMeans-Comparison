case class Cluster(points: Array[Point], var id : Int){
  override def toString: String = "Cluster id: " + id + " Num of points: " + points.length + " Points: (" + points.mkString(" ") + ")"

  def canEqual(a: Any): Boolean = a.isInstanceOf[Cluster]

  override def equals(that: Any): Boolean = that match {
    case that: Cluster => that.canEqual(this) && this.points.deep == that.points.deep && this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = {
    var sum: Double = 0
    this.points.foreach(p => p.dimensions.foreach(sum += _))
    this.id * 167 + this.points.length * 53 + sum.toInt
  }
}
