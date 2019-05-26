case class UnionFind(id: Array[Int], var count: Int) {

  def this(elements: Int){
    this(Array.range(0, elements.toInt) ,elements)
  }

  def find(element: Int): Int ={

    require(element >= 0 && element < this.id.length)

    var el: Int = element
    while (el != this.id(el)){
      el = this.id(el)
    }

    el
  }

  /*
    Returns true if the union was successful and reduces the number of elements in the structure
   */
  def union(left: Int, right: Int): Boolean ={

    val l: Int = this.find(left)
    val r: Int = this.find(right)

    val bigger: Int = {
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

  def pathCompression(element: Int, bigger: Int): Unit ={

    var el: Int = element
    var parent: Int = 0

    while (el != this.id(el)){
      parent = this.id(el)
      this.id(el) = bigger
      el = parent
    }

    this.id(el) = bigger
  }
}
