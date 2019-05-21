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
    Wrapper function to abstact insertion of point in tree
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
}
