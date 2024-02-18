package neeraj.spark.assignment

import org.apache.spark.sql.SparkSession
import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, DataFrame}
import scala.collection.mutable.WrappedArray


import org.apache.spark.sql.functions.countDistinct

case class flight(passengerID: Int, flightID: Int, from: String, to: String, date: java.sql.Date)
case class passenger(passengerID: Int, firstName: String, lastName: String)
case class flightt(passengerID: Int, flightID: Int, from: String, to: String, date: java.sql.Date, monthNumber: Int)
case class flightByMon(MonthNumber: Int, Count: BigInt)
case class joinedFlightPassenger (passengerID: Int, flightID: Int,from: String, to: String, date: java.sql.Date, firstName: String, lastName: String)
final case class flightPassenger (passengerID: Int, flightID: Int, firstName: String, lastName: String)
case class passengerFlights(passengerID: Int, longestRunOutsideUK: Int)


object Main {
  def main(args: Array[String]): Unit = {

    // creating spark context object

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate();

    import spark.implicits._

    //Getting the Flight data and passenger dataset ready.

    val path = "data/"
    val flightData = spark.read.option("header","true").option("inferSchema", "true").csv(path+"flightData.csv")
    val passengerData = spark.read.option("header","true").option("inferSchema", "true").csv(path+"passengers.csv")



    val flightEncoder = org.apache.spark.sql.Encoders.product[flight]
    val passengerEncoder = org.apache.spark.sql.Encoders.product[passenger]

    val flightDs = flightData.as(flightEncoder)
    val passengerDs = passengerData.as(passengerEncoder)

    flightDs.show()

// Question 1: Find the total number of flights for each month.


    val flightDs2  = flightDs.map(row => flightt(row.passengerID, row.flightID, row.from, row.to, row.date,
      row.date.toString.substring(5,7).toInt))


    // Get distinct count of the flightID using countDistinct for each month number

    val flightByMonth = flightDs2.groupBy("monthNumber")
      .agg(countDistinct("flightID").as("Count"))
      .orderBy("monthNumber")

    org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)

    val flightByMonthDs  = flightByMonth
    flightByMonthDs.show()

    flightByMonthDs.write.format("csv").mode("overwrite").option("header","true").save(path+"flightByMonthDs")



//  Question 2: Find the names of the 100 most frequent flyers.

    val joinedData = flightDs.join(passengerDs, Seq("passengerID"), "inner")
    val joinedFlightPassengerDs  = joinedData.as[joinedFlightPassenger]


    val flightPassengerDs = joinedFlightPassengerDs.map(row => flightPassenger(row.passengerID, row.flightID, row.firstName, row.lastName))
    val top100Flyers = flightPassengerDs.groupBy("passengerID","firstName","lastName")
      .agg(count("flightID").as("Number of Flights"))
      .orderBy(col("Number of Flights").desc)
      .limit(100)
    top100Flyers.show()

    top100Flyers.write.format("csv").option("header" , "true").mode("overwrite").save(path+"top100Flyers")


//Question 3: Find the greatest number of countries a passenger has been in without being in the UK. For example, if the countries a passenger was in were: UK -> FR -> US -> CN -> UK -> DE -> UK, the correct answer would be 3 countries.

    // Define a function to remove consecutive duplicates in an array, keeping only the first occurrence
    def removeConsecutiveDuplicates(arr: Seq[String]): Seq[String] = {
      arr.foldLeft(Seq.empty[String])
      { (acc, value) =>
        if (acc.isEmpty || acc.last != value) acc :+ value
        else acc
      }
    }

    // Register the UDF as we are using the UDF inside the withColumn.
    val removeConsecutiveDuplicatesUDF = udf((arr: Seq[String]) => removeConsecutiveDuplicates(arr))

    // In order to get all countries that passenger been to: get the "from" into an array and then last country from "to" and then contenate

    val resultDF = flightDs.groupBy("passengerID")
      .agg(collect_list("from").as("from"), last("to").as("to"))
      .withColumn("from", removeConsecutiveDuplicatesUDF(col("from")))
      .withColumn("concatenated_result", concat_ws(", ", col("from")))

    val finalResultDF = resultDF.select(
      col("passengerID"),
      split(concat_ws(", ", col("concatenated_result"), col("to")), ", ").alias("concatenated_array")
    )

    // function to get the max of total countries outside uk.

    def longestRunOutsideUK(Sqnce: Seq[String]):Int = {
      if (Sqnce.distinct == List("uk")) Array(0).max
      else {
        Sqnce.mkString(" ").split("uk").filter(_.nonEmpty).map(_.trim).map(s => s.split(" ").length).max
      }
    }

    val longestRunOutsideUkByPassenger = finalResultDF.map(r => passengerFlights(r(0).asInstanceOf[Int],longestRunOutsideUK(r(1).asInstanceOf[Seq[String]]))).orderBy(col("longestRunOutsideUK").desc)
    longestRunOutsideUkByPassenger.show(truncate = false)

    longestRunOutsideUkByPassenger.write.format("csv").option("header" , "true").mode("overwrite").save(path+"longestRunOutsideUkByPassenger")




//Question 4:Find the passengers who have been on more than 3 flights together.


    val flight1 = flightData
    val flight2 = flight1

    // self joining the two dataframes on the basis of flight id, date and passengerid

    def flightsTogetherFunc(flight1:DataFrame,flight2:DataFrame,N:Int) : DataFrame =
    {
      flight1.as("F1").join(flight2.as("F2"),
          $"F1.passengerID" < $"F2.passengerID"
            && $"F1.flightID" === $"F2.flightID"
            && $"F1.date" === $"F2.date"
          ,"inner")
        .groupBy($"F1.passengerID", $"F2.passengerID")
        .agg(count("*").alias("Number of Flight Together"))
        .filter($"Number of Flight Together" >= N)
        .orderBy(col("Number of Flight Together").desc)
        .select($"F1.passengerID".as("PassengerID1"), $"F2.passengerID".as("PassengerID2"),$"Number of Flight Together")
    }

    val flightsTogether : DataFrame = flightsTogetherFunc(flight1,flight2,3)
    flightsTogether.show()

    flightsTogether.repartition(1).write.format("csv").option("header" , "true").mode("overwrite").save(path+"flightsTogether")



//Question 5: Find the passengers who have been on more than N flights together within the range (from,to).

    def flightsTogetherFunc2(flight1:DataFrame,flight2:DataFrame,N:Int) : DataFrame = {
      flight1.as("F1").join(flight2.as("F2"),
          $"F1.passengerID" < $"F2.passengerID"
            && $"F1.flightID" === $"F2.flightID"
            && $"F1.date" === $"F2.date"
          ,"inner")
        .groupBy($"F1.passengerID", $"F2.passengerID")
        .agg(count("*").alias("Number of Flight Together"), min($"F1.date").as("from"), max($"F1.date").as("to") )
        .filter($"Number of Flight Together" >= N)
        .orderBy(col("Number of Flight Together").desc)
        .select($"F1.passengerID".as("passengerID1"), $"F2.passengerID".as("passengerID2"),$"from",$"to",$"Number of Flight Together")
    }

    val flightsTogetherWithDates : DataFrame = flightsTogetherFunc2(flight1,flight2,3)
    flightsTogetherWithDates.show()

    flightsTogetherWithDates.repartition(1).write.format("csv").option("header" , "true").mode("overwrite").save(path+"flightsTogetherWithDates")

  }
}
