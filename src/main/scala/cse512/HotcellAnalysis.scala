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
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART

  //we could get the list of rectangles .. then query with a group by date .. :)
  //this will give us the xi's
  //then will see how to go from there ..

  var x = minX.toInt
  var y = minY.toInt

  val step = HotcellUtils.coordinateStep

  val rectangles = scala.collection.mutable.ListBuffer.empty[String]

  while(x <= maxX){
    y = minY.toInt
    while(y <= maxY){
      rectangles += x.toString + "," + y.toString + "," + (x + 1).toString + "," + (y + 1).toString
      y += 1
    }
    x += 1
  }

  //now we have all the rectangles.
  //now creating the rectangle view then creating a view ..
  val df = rectangles.toDF("rectangle")

  //pickupInfo x, y, z
  pickupInfo.createOrReplaceTempView("pickups")

  val pickUps = spark.sql("select *, concat(x, ',', y) as point from pickups") //creating the point column from x & y
  pickUps.createOrReplaceTempView("points")

  df.createOrReplaceTempView("rectangles")

  spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
  val joinDf = spark.sql("select rectangles.rectangle as rectangle, points.point as point, points.z as z from rectangles, points where ST_Contains(rectangles.rectangle,points.point)")

  joinDf.createOrReplaceTempView("joinResult")
  val zone_counts_by_day = spark.sql("select rectangle, count(point) as sum, z from joinResult group by rectangle, z order by rectangle")

  println("printing zone_counts_by_day")
  zone_counts_by_day.show()

  //think for speed we need to convert to a map .. then we query from there .. df.filter simply is not making the cut..
  //df.select($"name", $"age".cast("int")).as[(String, Int)].collect.toMap

  //now for the calculations ... things are about to get messy .. hahahaha

  zone_counts_by_day.createOrReplaceTempView("my_data")
  val finalDF = spark.sql("select *, concat(rectangle, ',', z) as ID from my_data")

  val finalMap = finalDF.select($"ID", $"sum".cast("int")).as[(String, Int)].collect.toMap

  for (i <- 0 until rectangles.length){
    for (d <- 1 to 31) {
      val x_ = scala.collection.mutable.ListBuffer.empty[Integer]

      val rec_cood = rectangles(i).split(",")

      val x1 = rec_cood(0).toInt
      val y1 = rec_cood(1).toInt
      val x2 = rec_cood(2).toInt
      val y2 = rec_cood(3).toInt

      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + y1.toString + "," + (x2-1).toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + y1.toString + "," + (x2+1).toString + "," + y2.toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + (y1-1).toString + "," + x2.toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse (x1.toString + "," + (y1+1).toString + "," + x2.toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1-1).toString + "," + (x2-1).toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1-1).toString + "," + (y1+1).toString + "," + (x2-1).toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1-1).toString + "," + (x2+1).toString + "," + (y2-1).toString + "," + (d+1).toString, 0)).toString.toInt

      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + d.toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + (d-1).toString, 0)).toString.toInt
      x_ += (finalMap getOrElse ((x1+1).toString + "," + (y1+1).toString + "," + (x2+1).toString + "," + (y2+1).toString + "," + (d+1).toString, 0)).toString.toInt

      var sum = 0
      x_.foreach( sum += _ )
      print(sum.toString + ", ")
    }
  }

  return zone_counts_by_day.coalesce(1) // YOU NEED TO CHANGE THIS PART
}
}
