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

  //println("total number of cells : ", numCells)

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

  //println("number of rectangles :", rectangles.length)

  //now we have all the rectangles.
  //now creating the rectangle view then creating a view ..
  val df = rectangles.toDF("rectangle")

  //pickupInfo x, y, z
  pickupInfo.createOrReplaceTempView("pickups")


  val pickUps = spark.sql("select *, concat(x, ',', y) as point from pickups") //creating the point column from x & y
//  println("Length of pickups :", pickUps.count())
  pickUps.createOrReplaceTempView("points")

  df.createOrReplaceTempView("rectangles")

  spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotcellUtils.ST_Contains(queryRectangle, pointString)))
  val joinDf = spark.sql("select rectangles.rectangle as rectangle, points.point as point, points.z as z from rectangles, points where ST_Contains(rectangles.rectangle,points.point)")
//  println("Length of joinDF : ", joinDf.count())

  joinDf.createOrReplaceTempView("joinResult")
  val zone_counts_by_day = spark.sql("select rectangle, count(point) as sum, z from joinResult group by rectangle, z order by rectangle")


//  println("Printing pickups by cell and day: ")
//  zone_counts_by_day.show()

  //think for speed we need to convert to a map .. then we query from there .. df.filter simply is not making the cut..
  //df.select($"name", $"age".cast("int")).as[(String, Int)].collect.toMap

  //now for the calculations ... things are about to get messy .. hahahaha

  // :/ now looks like I will have to take care of the edge cases as well .. hahahaha that's a real bummer ..

  zone_counts_by_day.createOrReplaceTempView("my_data")
  val finalDF = spark.sql("select *, concat(rectangle, ',', z) as ID from my_data")


//  println("The final DF to be converted to the Hash :")
//  finalDF.show()

//  println("starting MAP conversion")
  val finalMap = finalDF.select($"ID", $"sum".cast("int")).as[(String, Int)].collect.toMap

//  println(finalMap)

  //now to calculate the mean and the standard deviation

  val sum_x = finalMap.values.sum.toDouble

  val x_bar = sum_x / numCells
  val s = Math.sqrt((finalMap.values.map(Math.pow(_, 2)).sum)/numCells - x_bar*x_bar)

//  println("the mean and sd: ", sum_x, x_bar, s)

  //ok; so now we have the mean and the sd right ..


  var finalMap_ = Map("test" -> 0.0)
//
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


      //there are a couple of edge cases to be dealt with
      //from the looks of things ... that will only be needed for the formula
      var n = 27

      implicit def bool2int(b:Boolean) = if (b) 1 else 0


      if (x1 == minX || x1 == maxX || y1 == minY || y1 == maxY || d == 1 || d == 31){
        val sum_n = !(x1%minX == 0) + !(x1%maxX == 0) + !(y1%minY == 0) + (y1%maxY == 0) + (d == 1 || d == 31)

        if (sum_n == 1){ n = 18}
        if (sum_n == 2){ n = 12}
        if (sum_n == 3){ n = 8}

      }


      //now that we have n .. lets calculate those values
      //n, sum, sum_2

      val G = (sum - x_bar*n)/(s * Math.sqrt((numCells*n - n*n)/(numCells - 1)))

//      finalMap_ += (x1.toString + "," + y1.toString + "," + x2.toString + "," + y2.toString + "," + d.toString -> G)
      finalMap_ += (x1.toString + "," + y1.toString + "," + d.toString -> G)

    }
  }
  var df_to_return  = finalMap_.toSeq.toDF("rectangle", "Gscore")

  df_to_return = df_to_return.withColumn("temp", split(col("rectangle"), ",")).select(
    col("*") +: (0 until 5).map(i => col("temp").getItem(i).as(s"col$i")): _*)

  return df_to_return.sort($"Gscore".desc).select("col0", "col1", "col2") // YOU NEED TO CHANGE THIS PART
}
}
