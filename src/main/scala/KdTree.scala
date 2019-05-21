import scala.math.min

case class Node(point: Point, var left: Node, var right: Node, var deleted: Boolean = false)

/*
  KD Tree with root as the root node and k dimensions for each point
 */
class KdTree(var root: Node, k: Int) {

  /*
    Creates a new node with no left and right children
   */
  def newNode(point: Point) = Node(point, null, null)

  /*
    Wrapper function to abstract insertion of point in tree
   */
  def insert(point: Point): Node = {
    insertRecursive(this.root, point, 0)
  }

  /*
    Insert point in tree recursively
   */
  def insertRecursive(root: Node, point: Point, depth: Int): Node = {
    if (root == null){
      return newNode(point)
    }

    // calculate current dimension
    val currentDim = depth % k

    // in case we insert a node that has been in the tree before, but it is now deleted
    if(this.arePointsSame(root.point, point)){
      root.point.cluster = point.cluster
      root.deleted = false // if the node was previously deleted, now we should add it again
    }
    else{
      // recurse for left child
      if(point.dimensions(currentDim) < root.point.dimensions(currentDim)){
        root.left = insertRecursive(root.left, point, depth + 1)
      }
      // recurse for right child
      else {
        root.right = insertRecursive(root.right, point, depth + 1)
      }
    }

    root
  }

  /*
    Finds if 2 points are the same
   */
  def arePointsSame(p1: Point, p2: Point): Boolean = {
    val d1 = p1.dimensions
    val d2 = p2.dimensions

    // find if there are any different dimensions between the two points
    val different = d1.indices.exists(dim => d1(dim) != d2(dim))

    !different
  }

  /*
    Wrapper function to abstract search for a point in tree
   */
  def search(point: Point): Boolean = {
    searchRecursive(this.root, point, 0)
  }

  def searchRecursive(root: Node, point: Point, depth: Int): Boolean = {
    if (root == null){
      return false
    }

    if (arePointsSame(root.point, point)){
      if (root.deleted){
        return false
      }
      else {
        return true
      }
    }

    // calculate current dimension
    val currentDim = depth % k

    // recurse for left child
    if(point.dimensions(currentDim) < root.point.dimensions(currentDim)){
      searchRecursive(root.left, point, depth + 1)
    }
    // recurse for right child
    else {
      searchRecursive(root.right, point, depth + 1)
    }
  }

  /*
    Wrapper function to abstract find minimum for a given dimension
   */
  def findMinimum(dimension: Int): Node = {
    findMinimumRecursive(this.root, dimension, 0)
  }

  def findMinimumRecursive(root: Node, dimension: Int, depth: Int) : Node = {

    if (root == null){
      return null
    }

    // calculate current dimension
    val currentDim = depth % k

    if(currentDim == dimension){
      if(root.left == null){
        return root
      }
      return minOfNodes(root, findMinimumRecursive(root.left, dimension, depth + 1), dimension)
    }

    // if current dimension is different than minimum then minimum can be anywhere
    minOfNodes(root, findMinimumRecursive(root.left, dimension, depth + 1), findMinimumRecursive(root.right, dimension, depth + 1), dimension)
  }

  /*
    Returns the minimum of 2 nodes along the specified axis
   */
  def minOfNodes(node1: Node, node2: Node, dimension: Int): Node = {
    if(node2 != null && node2.point.dimensions(dimension) < node1.point.dimensions(dimension)){
      node2
    }
    else {
      node1
    }
  }

  /*
    Returns the minimum of 3 nodes along the specified axis
   */
  def minOfNodes(node1: Node, node2: Node, node3: Node, dimension: Int) : Node = {
    val minOfTwo = minOfNodes(node1, node2, dimension)
    val total = minOfNodes(minOfTwo, node3, dimension)

    total
  }
}
