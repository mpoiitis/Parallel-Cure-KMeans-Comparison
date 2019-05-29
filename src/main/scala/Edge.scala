case class Edge(left: Point, right: Point, weight: Double) {
  override def toString: String = "[(" + this.left + ", " + this.right + ") " + this.weight + "]"
}
