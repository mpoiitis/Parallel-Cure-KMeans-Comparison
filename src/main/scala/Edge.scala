case class Edge(left: Long, right: Long, weight: Double) {
  override def toString: String = "[(" + this.left + ", " + this.right + ") " + this.weight + "]"
}
