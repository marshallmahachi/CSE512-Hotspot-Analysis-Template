package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART

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

 //
}
