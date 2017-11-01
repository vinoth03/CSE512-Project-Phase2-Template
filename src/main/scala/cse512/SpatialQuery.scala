package cse512

import org.apache.spark.sql.SparkSession

import scala.math.pow

object SpatialQuery extends App{

  def ST_Contains(queryRectangle:String, pointString:String): Boolean = {
    val rect:Array[String] = queryRectangle.split(",")
    val point:Array[String] = pointString.split(",")

    val rectD = rect.map( x => x.toDouble)
    val pointD = point.map( x => x.toDouble)

    val rx1 = rectD(0)
    val ry1 = rectD(1)
    val rx2 = rectD(2)
    val ry2 = rectD(3)

    val px = pointD(0)
    val py = pointD(1)

    if( rx1 > rx2 && ry1 > ry2)
    {
      if( rx2 <= px && px <= rx1 && ry2 <= py && py <= ry1 )
      {
        return true
      }
    }
    else if( rx1 < rx2 && ry1 > ry2)
    {
      if( rx2 >= px && px >= rx1 && ry2 <= py && py <= ry1 )
      {
        return true
      }
    }
    else if( rx1 < rx2 && ry1 < ry2)
    {
      if( rx2 >= px && px >= rx1 && ry2 >= py && py >= ry1 )
      {
        return true
      }
    }
    else if( rx1 > rx2 && ry1 < ry2)
    {
      if( rx2 <= px && px <= rx1 && ry2 >= py && py >= ry1 )
      {
        return true
      }
    }

    return false
  }

  def ST_Within(str1: String, str2: String, dist: Double): Boolean ={
    val values1 = str1.split(",").map(_.toDouble)
    val values2 = str2.split(",").map(_.toDouble)
    val left = pow(values1(0) - values2(0), 2) + pow(values1(1) - values2(1), 2)
    val right = pow(dist,2)
    return left <= right
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle,pointString))))

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
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(queryRectangle,pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    //val compareWithin = udf { s: String => s.toUpperCase }

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1,pointString2,distance)))

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
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1,pointString2,distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
