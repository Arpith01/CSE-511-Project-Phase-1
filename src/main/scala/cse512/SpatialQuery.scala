package cse512

import org.apache.spark.sql.SparkSession
import scala.math

object SpatialQuery extends App{
  def st_contains_def(queryRectangle:String, pointString:String): Boolean  = {
    var rect = queryRectangle.split(",")
    var x1 = rect(0).toDouble
    var y1 = rect(1).toDouble
    var x2 = rect(2).toDouble
    var y2 = rect(3).toDouble

    var point = pointString.split(",")
    var p_x = point(0).toDouble
    var p_y = point(1).toDouble

    var result1:Boolean = true
    if(x1<=x2)
    {
      if(p_x>=x1 && p_x<=x2){
        result1 = result1 && true
      }
      else
      {
        result1 = false
      }
    }
    else
    {
      if(p_x>=x2 && p_x<=x1){
        result1 = result1 && true
      }
      else
      {
        result1 = false
      }
    }

    var result2:Boolean = true

    if(y1<=y2)
    {
      if(p_y>=y1 && p_y<=y2){
        result2 = result2 && true
      }
      else
      {
        result2 = false
      }
    }
    else{
      if(p_x>=y2 && p_x<=y1){
        result2 = result2 && true
      }
      else
      {
        result2 = false
      }
    }

    return result1 && result2
  }

  def st_within_def(pointString1:String, pointString2:String, distance:Double): Boolean = {
    var point1 = pointString1.split(",")
    var x1 = point1(0).toDouble
    var y1 = point1(1).toDouble
    
    var point2 = pointString2.split(",")
    var x2 = point2(0).toDouble
    var y2 = point2(1).toDouble

    var calc_dist = math.sqrt(math.pow((x1-x2),2) + math.pow((y1-y2),2))
    if(calc_dist<=distance) return true
    return false
  }


  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains_def(queryRectangle, pointString))))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((st_contains_def(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_within_def(pointString1, pointString2, distance))))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((st_within_def(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
