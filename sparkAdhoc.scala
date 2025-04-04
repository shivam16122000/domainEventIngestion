/opt/homebrew/Cellar/apache-spark/3.5.1/bin/spark-shell \
  --packages io.delta:delta-spark_2.12:3.1.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import io.delta.tables._

val spark = SparkSession.builder
  .appName("DomainEventIngestion")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

val deltaTablePath = "hdfs://localhost:9001/delta/order_events"

spark.sql(s"CREATE TABLE order_events USING DELTA LOCATION '$deltaTablePath'")

val resultDF = spark.sql("SELECT * FROM order_events")
resultDF.show()

 spark.sql("SELECT * FROM order_events version as of 1").show(false)
 spark.sql("SELECT * FROM order_events").show(false)

 // Optimize Delta table
spark.sql(s"OPTIMIZE delta.`$deltaTablePath` ZORDER BY (order_id)")

// Vacuum old versions
spark.sql(s"VACUUM delta.`$deltaTablePath` RETAIN 168 HOURS")
