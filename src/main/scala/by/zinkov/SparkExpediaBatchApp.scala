package by.zinkov

import java.util.Properties

import by.zinkov.beans.{HotelIdCheckins, Visits}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object SparkExpediaBatchApp {

  val KAFKA_TOPIC_PROPERTY: String = "kafka.topic"
  val KAFKA_BOOTSTRAP_SERVER_PROPERTY: String = "kafka.bootstrap.server"
  val KAFKA_STARTING_OFFSET_PROPERTY: String = "kafka.starting.offset"
  val KAFKA_ENDING_OFFSET_PROPERTY: String = "kafka.ending.offset"
  val HADOOP_EXPEDIA_FOLDER_PROPERTY: String = "hadoop.expedia.folder.url"
  val HADOOP_EXPEDIA_VALID_FOLDER_PROPERTY: String = "hadoop.expedia.valid.folder.url"


  val jsonSchema: StructType = StructType(
    StructField("Id", LongType)
      :: StructField("Country", StringType)
      :: StructField("City", StringType)
      :: StructField("Address", StringType)
      :: StructField("Name", StringType)
      :: Nil)

  def main(args: Array[String]): Unit = {

    val properties = new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("app.properties"))

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkExpediaBatchApp")
      .getOrCreate();

    println("First SparkContext:")
    println("APP Name :" + spark.sparkContext.appName);
    println("Deploy Mode :" + spark.sparkContext.deployMode);
    println("Master :" + spark.sparkContext.master);

    import spark.implicits._
    /**
     * Read hotels&weather data from Kafka with Spark application in a batch manner
     * (by specifying start offsets and batch limit in configuration file).
     */
    val kafka = spark
      .read
      .format("kafka")
      .option("kafka.bootstrap.servers", properties.getProperty(KAFKA_BOOTSTRAP_SERVER_PROPERTY))
      .option("subscribe", properties.getProperty(KAFKA_TOPIC_PROPERTY))
      .option("startingOffsets", properties.getProperty(KAFKA_STARTING_OFFSET_PROPERTY))
      .load()

    val hotels = kafka.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value").cast("string"), jsonSchema) as "parsed_value")
      .select("parsed_value.*")


    /**
     * Read Expedia data from HDFS with Spark.
     * */

    val avro = spark
      .read
      .format("avro")
      .load(properties.getProperty(HADOOP_EXPEDIA_FOLDER_PROPERTY))

    val hotelsIdCheckins = avro
      .select(col("id")
        , col("srch_ci")
        , col("srch_co")
        , col("hotel_id"))
      .as[Visits]
      .groupBy(col("hotel_id"))
      .agg(sort_array(collect_list(col("srch_ci"))) as "checkins")
      .as[HotelIdCheckins]

    /**
     * Remove all booking data for hotels with at least one "invalid" row (with idle days more than or equal to 2 and less than 30).
     */
    val validHotels = hotelsIdCheckins
      .filter(_.idleDaysIsValid())
      .join(hotels, $"hotel_id" === $"Id", "inner")
      .drop("checkins")

    /**
     * Remove all booking data for hotels with at least one "invalid" row (with idle days more than or equal to 2 and less than 30).
     * Print results
     */
    validHotels
      .drop("hotel_id")
      .show()

    /**
     * Print hotels info (name, address, country etc) of "invalid" hotels and make a screenshot. Join expedia and hotel data for this purpose.
     * */
    hotelsIdCheckins
      .filter(!_.idleDaysIsValid())
      .join(hotels, $"hotel_id" === $"Id", "inner")
      .drop("checkins", "hotel_id")
      .show()


    /**
     * Group the remaining data and print bookings counts.
     */
    /**
     * by hotel country
     */
    avro
      .select(col("id") as "exp_id"
        , col("hotel_id"))
      .join(validHotels.select(col("Id"), col("Country")), $"hotel_id" === $"Id", "inner")
      .groupBy(col("Country"))
      .count()
      .show()

    /**
     * by hotel city
     */
    avro
      .select(col("id") as "exp_id"
        , col("hotel_id"))
      .join(validHotels.select(col("Id"), col("City")), $"hotel_id" === $"Id", "inner")
      .groupBy(col("City"))
      .count()
      .show(100)


    /**
     * Store "valid" Expedia data in HDFS partitioned by year of "srch_ci".
     */

    avro
      .join(validHotels.select(col("Id") as "valid_hotel_id"), $"hotel_id" === $"valid_hotel_id", "inner")
      .drop(col("valid_hotel_id"))
      .repartition(1)
      .withColumn("year", substring_index(col("srch_ci"), "-", 1))
      .write
      .partitionBy("year")
      .format("avro")
      .save(properties.getProperty(HADOOP_EXPEDIA_VALID_FOLDER_PROPERTY))

  }
}
