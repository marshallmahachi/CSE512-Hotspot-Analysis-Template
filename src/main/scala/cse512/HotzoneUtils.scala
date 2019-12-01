package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangle = queryRectangle.split(",")
    val point = pointString.split(",")

    var returnBool = false
    var rect_x1 = rectangle(0).trim.toDouble
    var rect_y1 = rectangle(1).trim.toDouble

    var rect_x2 = rectangle(2).trim.toDouble
    var rect_y2 = rectangle(3).toDouble

    var point_x = point(0).trim.toDouble
    var point_y = point(1).trim.toDouble

    var lower_bound_x: Double = rect_x1
    var upper_bound_x: Double = rect_x1

    var lower_bound_y: Double = rect_y1
    var upper_bound_y: Double = rect_y1


    if (rect_x1 > rect_x2){
      lower_bound_x = rect_x2
      upper_bound_x = rect_x1
    } else {
      lower_bound_x = rect_x1
      upper_bound_x = rect_x2
    }

    if (rect_y1 > rect_y2){
      lower_bound_y = rect_y2
      upper_bound_y = rect_y1
    } else {
      lower_bound_y = rect_y1
      upper_bound_y = rect_y2
    }

    //then run the condition now
    if (point_x >= lower_bound_x && point_x < upper_bound_x && point_y >= lower_bound_y && point_y < upper_bound_y){
      returnBool = true
    }

    returnBool
  }
  // YOU NEED TO CHANGE THIS PART
}
