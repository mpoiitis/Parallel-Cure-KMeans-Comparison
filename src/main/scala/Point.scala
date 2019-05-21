case class Point ( dimensions : Array[Double], var cluster : Cluster = null) {
  override def toString: String = {
    var str = "("
    this.dimensions.foreach(dim => str += dim + ", ")
    str += ")"
    str
  }
}

