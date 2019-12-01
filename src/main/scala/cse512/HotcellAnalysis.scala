package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  import spark.sqlContext.implicits._
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = (-74.50/HotcellUtils.coordinateStep).toInt
  val maxX = (-73.70/HotcellUtils.coordinateStep).toInt
  val minY = (40.50/HotcellUtils.coordinateStep).toInt
  val maxY = (40.90/HotcellUtils.coordinateStep).toInt
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  println("total number of cells : ", numCells)

  // YOU NEED TO CHANGE THIS PART

  //we could get the list of rectangles .. then query with a group by date .. :)
  //this will give us the xi's
  //then will see how to go from there ..

  var x = minX.toInt
  var y = minY.toInt

  val rectangles = scala.collection.mutable.ListBuffer.empty[String]

  while(x <= maxX){
    y = minY.toInt
    while(y <= maxY){
      rectangles += x.toString + "," + y.toString + "," + (x + 1).toString + "," + (y + 1).toString
      y += 1
    }
    x += 1
  }

  println("number of rectangles :", rectangles.length)

  //now we have all the rectangles.
  //now creating the rectangle view then creating a view ..
  val df = rectangles.toDF("rectangle")

  //pickupInfo x, y, z
  pickupInfo.createOrReplaceTempView("pickups")


  val pickUps = spark.sql("select *, concat(x, ',', y) as point from pickups") //creating the point column from x & y
  println("Length of pickups :", pickUps.count())
  pickUps.createOrReplaceTempView("points")

  df.createOrReplaceTempView("rectangles")

  spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
  val joinDf = spark.sql("select rectangles.rectangle as rectangle, points.point as point, points.z as z from rectangles, points where ST_Contains(rectangles.rectangle,points.point)")
  println("Length of joinDF : ", joinDf.count())

  joinDf.createOrReplaceTempView("joinResult")
  val zone_counts_by_day = spark.sql("select rectangle, count(point) as sum, z from joinResult group by rectangle, z order by rectangle")


  println("Printing pickups by cell and day: ")
  zone_counts_by_day.show()

  //think for speed we need to convert to a map .. then we query from there .. df.filter simply is not making the cut..
  //df.select($"name", $"age".cast("int")).as[(String, Int)].collect.toMap

  //now for the calculations ... things are about to get messy .. hahahaha

  // :/ now looks like I will have to take care of the edge cases as well .. hahahaha that's a real bummer ..

  zone_counts_by_day.createOrReplaceTempView("my_data")
  val finalDF = spark.sql("select *, concat(rectangle, ',', z) as ID from my_data")

  println("The final DF to be converted to the Hash :")
  finalDF.show()

//  val finalMap = finalDF.select($"ID", $"sum".cast("int")).as[(String, Int)].collect.toMap
//  var finalMap_ = Map("test" -> 0.0)
//
//  for (i <- 0 until rectangles.length){
//    for (d <- 1 to 31) {
//      val x_ = scala.collection.mutable.ListBuffer.empty[Integer]
//
//      val rec_cood = rectangles(i).split(",")
//
//      val x1 = rec_cood(0).toInt
//      val y1 = rec_cood(1).toInt
//      val x2 = rec_cood(2).toInt
//      val y2 = rec_cood(3).toInt
//
//      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
//      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt
//
//      var sum = 0
//      var sum_2 = 0
//      x_.foreach( sum += _ )
//
//      var b=0
//      while (b < x_.length) {
//        sum_2 += x_(b)*x_(b)
//        b += 1
//      }
//
//      //there are a couple of edge cases to be dealt with
//      //from the looks of things ... that will only be needed for the formula
//      var n = 0
//      if (x1 == minX || x1 == maxX || y1 == minY || y1 == maxY || d == 1 || d == 31){
//        //there 8 cases for the cubes at the corners with 8 neighbors
//        if (x1 == maxX & y1 == minY){
//          if(d == 1 || d == 31){ n = 8 } else { n = 12 }
//        }
//
//        if (x1 == maxX & y1 == maxY){
//          if(d == 1 || d == 31){ n = 8 } else { n = 12 }
//        }
//
//        if (x1 == minX & y1 == minY){
//          if(d == 1 || d == 31){ n = 8 } else { n = 12 }
//        }
//
//        if (x1 == minX & y1 == maxY){
//          if(d == 1 || d == 31){ n = 8 } else { n = 12 }
//        }
//
//        //the rest of the cubes at the edges
//      } else {
//        n = 27
//      }
//
//
//      //now that we have n .. lets calculate those values
//      //n, sum, sum_2
//      val x_bar = sum/n
//      val s = Math.sqrt(sum_2/n - x_bar*x_bar)
//
//      val G = (sum - x_bar*n)/(s * Math.sqrt((n*n - n*n)/(n - 1)))
//
//      finalMap_ += (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + d.toString -> G)
//
//    }
//  }
//
//  val df_to_return  = finalMap_.toSeq.toDF("rectangle", "Gscore")

//  return joinDf.sort($"Gscore".desc).coalesce(1) // YOU NEED TO CHANGE THIS PART
  return finalDF
}
}
