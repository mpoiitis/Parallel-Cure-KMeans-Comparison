case class UnionFind(id: Array[Long], count: Long) {

  def this(elements: Long){
    this(Array.range(0, elements.toInt) ,elements)
  }

  def find(element: Long): Long ={

    require(element >= 0 && element < this.id.length)

    var el: Long = element
    while (el != this.id(el)){
      el = this.id(el)
    }

    el
  }

  /*
    Returns true if the union was successful and reduces the number of elements in the structure
   */
  def union(left: Long, right: Long): Boolean ={

    val l: Long = this.find(left)
    val r: Long = this.find(right)

    val bigger: Long = {
      if (l > r) l
      else r
    }

    pathCompression(left, bigger)
    pathCompression(right, bigger)

    if (l == r) false
    else {
      count -= 1
      true
    }

  }

  /*
    Flatten the structure
    Every point on the way to the root (bigger) will point to the root because they belong to the same set
    Speeds up future operations
   */

  def pathCompression(element: Long, bigger: Long): Unit ={

    var el: Long = element
    var parent: Long = 0

    while (el != this.id(el)){
      parent = this.id(el)
      this.id(el) = bigger
      el = parent
    }

    this.id(el) = bigger
  }
}
