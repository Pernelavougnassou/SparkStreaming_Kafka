import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType}

object Main {

  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    // create spark session with settings
    //System.setProperty("hadoop.home.dir","C:\\hadoop")
    val spark = SparkSession
      .builder
      //.master("local")
      .appName("SparkStreaming_Kafka")
      .getOrCreate()
    //spark.conf.set("spark.hadoop.parquet.enable.summary-metadata", false)
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // create kafka streaming data frame
    val kafkaDS = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "m10223.contaboserver.net:6667")
      .option("subscribe", "covid_input_pernel")
      .option("startingOffsets","earliest")
      .option("failOnDataLoss", "false")
      .load()
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    val schema = new StructType()
      .add("date_cas", StringType, true)
      .add("variant", StringType, true)
      .add("id_pays", StringType, true)

    val refoemated_df = kafkaDS.withColumn("struct_value", from_json(col("value"), schema))
    val explode_df = refoemated_df.withColumn("variant", col("struct_value").getField("variant"))

    explode_df
      .writeStream
      .outputMode("update")
      .option("truncate", false)
      .format("console")
      .start()
      .awaitTermination()
  }
}