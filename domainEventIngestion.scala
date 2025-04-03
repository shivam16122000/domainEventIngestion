/opt/homebrew/Cellar/apache-spark/3.5.1/bin/spark-shell \
  --packages "io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.prometheus:simpleclient:0.16.0,io.prometheus:simpleclient_httpserver:0.16.0" \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

import org.apache.spark.sql.{SparkSession, DataFrame, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types._
import io.delta.tables._
import java.util.concurrent.TimeUnit
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.{CollectorRegistry, Counter, Gauge}
import java.net.InetSocketAddress


val spark = SparkSession.builder
  .appName("DomainEventIngestion")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .config("spark.metrics.conf.*.sink.prometheus.class", "org.apache.spark.metrics.sink.PrometheusSink")
  .config("spark.ui.prometheus.enabled", "true")
  .getOrCreate()

import spark.implicits._

val registry = new CollectorRegistry(true)

val rowsUpdatedCounter = Counter.build()
.name("order_events_rows_updated")
.help("Total rows updated in Delta Lake")
.register(registry)

val rowsInsertedCounter = Counter.build()
.name("order_events_rows_inserted")
.help("Total rows inserted in Delta Lake")
.register(registry)

val processingLagGauge = Gauge.build()
.name("order_events_processing_lag_seconds")
.help("Event time processing lag in seconds")
.register(registry)

val prometheusServer = new HTTPServer(new InetSocketAddress(9093), registry)


val orderSchema = new StructType()
    .add("order_id", StringType, nullable = false)
    .add("status", StringType)
    .add("event_time", TimestampType)
    .add("amount", DoubleType)


val kafkaDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "localhost:9092")
  .option("subscribe", "order_events")
  .option("startingOffsets", "earliest")
  .option("maxOffsetsPerTrigger", "1000")
  .load()

val parsedDF = kafkaDF
      .select(from_json($"value".cast(StringType), orderSchema).as("data"))
      .select("data.*")
      .withColumn("event_date", date_format($"event_time", "yyyy-MM-dd"))
      .withColumn("processing_time", current_timestamp())


val deltaTablePath = "hdfs://localhost:9001/delta/order_events"
val checkpointLocation = "hdfs://localhost:9001/delta/order_events_checkpoint"

private def setupDeltaTable(path: String, schema: StructType): Unit = {
    val emptyRDD: RDD[Row] = spark.sparkContext.emptyRDD[Row]
    if (!DeltaTable.isDeltaTable(spark, path)) {
      spark.createDataFrame(emptyRDD, schema)
        .write
        .format("delta")
        .partitionBy("event_date")
        .save(path)
    }
}

setupDeltaTable(deltaTablePath, parsedDF.schema)

def monitorQuery(query: StreamingQuery): Unit = {
      new Thread(() => {
        while (query.isActive) {
          val progress = query.lastProgress
          if (progress != null) {
            // Update processing lag metric
            val timestampMillis = java.time.Instant.parse(progress.timestamp).toEpochMilli
            val lagSeconds = (System.currentTimeMillis() - timestampMillis) / 1000
            processingLagGauge.set(lagSeconds)
          }
          Thread.sleep(10000) // Update every 10 seconds
        }
      }).start()
    }

def deduplicateAndMerge(batchDF: DataFrame, batchId: Long): Unit = {
  import org.apache.spark.sql.expressions.Window
  import org.apache.spark.sql.functions._

  // Deduplicate within the micro-batch
  val dedupDF = batchDF.withColumn("row_num", 
      row_number().over(
        Window.partitionBy("order_id")
              .orderBy(col("event_time").desc)
      ))
    .filter("row_num = 1")
    .drop("row_num")

  // Then merge
  mergeBatch(dedupDF, batchId) 
}

// Merge function with metrics
def mergeBatch(batchDF: DataFrame, batchId: Long): Unit = {
    val deltaTable = DeltaTable.forPath(spark, deltaTablePath)
    
    val mergeResult = deltaTable.as("target")
    .merge(
        batchDF.as("source"),
        "target.order_id = source.order_id")
    .whenMatched("source.event_time > target.event_time")
    .updateAll()
    .whenNotMatched()
    .insertAll()
    .execute()

      val metrics = deltaTable.history(1).select("operationMetrics").collect()(0)
    .getMap[String, String](0)

    // Extract and update counters
    val rowsUpdated = metrics("numTargetRowsUpdated").toLong
    val rowsInserted = metrics("numTargetRowsInserted").toLong

    rowsUpdatedCounter.inc(rowsUpdated)
    rowsInsertedCounter.inc(rowsInserted)

}

val query = parsedDF.writeStream
      .outputMode("update")
      .foreachBatch(deduplicateAndMerge _)
      .option("checkpointLocation", checkpointLocation)
      .trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))
      .start()

// Monitoring setup
monitorQuery(query)

query.awaitTermination()



