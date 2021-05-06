import StreamWeather.table2
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Get

object StreamWeather {
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  val hbaseConf: Configuration = HBaseConfiguration.create()
  hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
  hbaseConf.set("hbase.zookeeper.quorum", "localhost")

  val hbaseConnection = ConnectionFactory.createConnection(hbaseConf)
  val table = hbaseConnection.getTable(TableName.valueOf("latest_chess_results"))
  val table2 = hbaseConnection.getTable(TableName.valueOf("shabtai_id_count"))


  def getLatestWeather(fide_id: String) = {
    val result = table.get(new Get(Bytes.toBytes(fide_id)))
    System.out.println(result.isEmpty())
    if(result.isEmpty())
      None
    else
        fide_id}

        //Bytes.toBoolean(result.getValue(Bytes.toBytes("stats"), Bytes.toBytes("games_count")))))
    
  def incrementDelaysByRoute(kfr : WeatherReport) : String = {
    // val maybeLatestWeather = kfr.fide_id
    val inc = new Increment(Bytes.toBytes(kfr.fide_id))
    inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("games_count"), 1)
    table2.increment(inc)
    return "Updated speed layer for player" + kfr.fide_id
  }


  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println(s"""
                            |Usage: StreamFlights <brokers>
                            |  <brokers> is a list of one or more Kafka brokers
                            |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers) = args

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("StreamWeather")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set("shabtai_final_test")
    // Create direct kafka stream with brokers and topics
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc, PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    )

    // Get the lines, split them into words, count the words and print
    val serializedRecords = stream.map(_.value);
    val reports = serializedRecords.map(rec => mapper.readValue(rec, classOf[WeatherReport]))

    // How to write to an HBase table
    val batchStats = reports.map(wr => {
      val put = new Put(Bytes.toBytes(wr.fide_id))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("first_name"), Bytes.toBytes(wr.first_name))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("last_name"), Bytes.toBytes(wr.last_name))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("rating_standard"), Bytes.toBytes(wr.rating_standard))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("rating_rapid"), Bytes.toBytes(wr.rating_rapid))
      put.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("rating_blitz"), Bytes.toBytes(wr.rating_blitz))
      table.put(put)

      // inc the other table

      // extract this into a function:

      // val inc = new Increment(Bytes.toBytes(wr.fide_id))
      // inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("games_count"), 1)
      // table2.increment(inc)

    })
    batchStats.print()

    // How to write to an HBase table

    val kfrs = serializedRecords.map(rec => mapper.readValue(rec, classOf[WeatherReport]))

    // Update speed table
    val processedFlights = kfrs.map(incrementDelaysByRoute)
    processedFlights.print()


    // var processedFlights = kfrs.map(incrementDelaysByRoute)
    /*
    val reports2 = serializedRecords.map(rec => mapper.readValue(rec, classOf[WeatherReport2]))
    val batchStats2 = reports2.map(wr => {
      val inc = new Increment(Bytes.toBytes(wr.fide_id))
      inc.addColumn(Bytes.toBytes("stats"), Bytes.toBytes("games_count"), 1)
      table2.increment(inc)
    })
    batchStats2.print()

     */


    // Start the computation
    ssc.start()
    ssc.awaitTermination()
  }

}
