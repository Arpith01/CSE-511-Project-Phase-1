package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{
  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stContains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(stContains(pointString, queryRectangle)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stWithin(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(stWithin(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

   def stContains(pointString:String, queryRectangle:String) : Boolean = {
    val point = pointString.split(",")
    val pointx = point(0).trim().toDouble
    val pointy = point(1).trim().toDouble
    
    val rectangle = queryRectangle.split(",")
    val rectx1 = rectangle(0).trim().toDouble
    val recty1 = rectangle(1).trim().toDouble
    val rectx2 = rectangle(2).trim().toDouble
    val recty2 = rectangle(3).trim().toDouble
    
    var min_x: Double = 0
    var max_x: Double = 0
    if(rectx1 < rectx2) 
    {
      min_x = rectx1
      max_x = rectx2
    } 
    else 
    {
      min_x = rectx2
      max_x = rectx1
    }
    
    var min_y: Double = 0
    var max_y: Double = 0
    if(recty1 < recty2) 
    {
      min_y = recty1
      max_y = recty2
    } 
    else 
    {
      min_y = recty2
      max_y = recty1
    }
    if(pointx >= min_x && pointx <= max_x && pointy >= min_y && pointy <= max_y) 
    {
      return true
    }
    else
    {
      return false
    }
  }

  def stWithin(pointString1:String, pointString2:String, distance:Double) : Boolean = {
    val point1 = pointString1.split(",")
    val pointx1 = point1(0).trim().toDouble
    val pointy1 = point1(1).trim().toDouble
    
    val point2 = pointString2.split(",")
    val pointx2 = point2(0).trim().toDouble
    val pointy2 = point2(1).trim().toDouble
    
    val euclidean_distance = scala.math.pow(scala.math.pow((pointx1 - pointx2), 2) + scala.math.pow((pointy1 - pointy2), 2), 0.5)
    
    return euclidean_distance <= distance
  }

}
