import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.SparkConf
import com.memsql.spark.connector.{rdd, _}
import com.memsql.spark.connector.util.MemSQLConnectionInfo
import com.memsql.spark.connector.util.JDBCImplicits._



object StockConsumer {

  val KAFKA_BROKER = ""
  val GROUP_ID = "stock_group_id"


  /** Initialze memsql connection info and spark context.
    *
    *  @return conf:  SparkConf  Configuration set with memsql connection info such as username, password, port, database and table info
    */
  def init(): SparkConf = {
    val connInfo = MemSQLConnectionInfo("", 3306, "", "", "")

    MemSQLConnectionPool.createPool(connInfo)
    MemSQLConnectionPool.connect(connInfo)

    //Create database and table if it doesn't exist already
    MemSQLConnectionPool.withConnection(connInfo)(conn => {
      conn.withStatement(stmt => {
        stmt.execute("CREATE DATABASE IF NOT EXISTS finaldb")
        stmt.execute("CREATE TABLE IF NOT EXISTS finaldb.finaltab (id INT PRIMARY KEY AUTO_INCREMENT, Symbol VARCHAR(15) NOT NULL, Name VARCHAR(200), LastSale DOUBLE, MarketCap DOUBLE, ADR_TSO VARCHAR(200), IPOYear VARCHAR(10), Sector Varchar(200), Industry VARCHAR(200), SummaryQuote VARCHAR(200), Price DOUBLE, Timestamp VARCHAR(20), d_datetime TIMESTAMP )")

      })
    })

    val conf = new SparkConf()
      .setAppName("Stream and Write stock data to memsql")
      .set("spark.memsql.host", connInfo.dbHost)
      .set("spark.memsql.port", connInfo.dbPort.toString)
      .set("spark.memsql.user", connInfo.user)
      .set("spark.memsql.password", connInfo.password)
      .set("spark.memsql.defaultDatabase", connInfo.dbName)

    return conf
  }

  /** Creating DStream that listens to Kafka topic 'topic-stock'
    *
    *  @param ssc: StreamingContext
    *
    *  @return InputDStream[ConsumerRecord[String, String ] ]   DStream listening to Kafka topic
    */

  def consumeKafkaStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> KAFKA_BROKER,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> GROUP_ID,
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("topic-stock")

    KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

  }

  /** Converts each field that is passed in as string to the desired data type
    *
    *  @param line: List[String] Streaming data that has been slpit into fields
    *
    *  @return Row: All fields in their correct data types as a Row type
    */
  def row(line: List[String]): Row = {

    //converting each field that is passed in as string to the desired data type
    Row(line(0), line(1), line(2).toDouble, line(3).toDouble, line(4), line(5), line(6), line(7), line(8), line(9).toDouble,line(10))
  }


  /** Runs a SparkStreaming job listening to Kafka topic "topic-stock"
    * The Kafka topic streams in data; this job processes this data
    * and saves the results to memsql.
    */
  def main(args: Array[String]): Unit = {


    //Initialize memsql connection info

    val sconf = init()


    val spark = SparkSession.builder().config(sconf).getOrCreate()
    // start new streaming context
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))


    val stream = consumeKafkaStream(ssc)

    //structure of data coming in
    val jschema = new StructType()
      .add("Symbol", StringType,false)
      .add("Name", StringType,false)
      .add("LastSale", DoubleType,false)
      .add("MarketCap", DoubleType,false)
      .add("ADR_TSO", StringType,false)
      .add("IPOyear" , StringType,false)
      .add("Sector", StringType,false)
      .add("Industry", StringType,false)
      .add("SummaryQuote", StringType,false)
      .add("Price", DoubleType, false)
      .add("Timestamp", StringType, false)

    stream.foreachRDD(
      rdd => {
        import org.apache.spark.sql._
        import org.apache.spark.sql.functions._
        import org.apache.spark.sql.types._

        print(rdd.map(_.value().split(",").to[List]))
        //strip off any extra quotes and split the string coming in into different fields and map to corresponding data type
        val data = rdd.map(_.value().replaceAll("'", "").replaceAll("\"", "").split(",").to[List]).map(row)

        val df = spark.createDataFrame(data, jschema)

        //converting the time string into the "Timestamp" datatype
        val timestamp2datetype: (Column) => Column = (x) => { to_date(x) }
        val new_df = df.withColumn("d_datetime", timestamp2datetype(col("Timestamp")))

        df.show()
        //saving the dataframe to Memsql
        df.saveToMemSQL("finaldb","finaltab")
      }
    )


    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }


}



