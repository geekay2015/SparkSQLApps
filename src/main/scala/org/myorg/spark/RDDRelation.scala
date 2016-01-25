package org.myorg.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

//define the schema of RDD using case class
case class Record(key: Int, value: String)

//Application main object
object RDDRelation {

  def main(args: Array[String]) {
    //Define the Spark Configuration
    val conf = new SparkConf()
      .setAppName("RDD Relation")
      .setMaster("local")

    //Define the Spark Context
    val sc = new SparkContext(conf)

    //Define the SQL Context
    val sqlContext = new SQLContext(sc)

    //import the SQL implicit conversions
    import sqlContext.implicits._

    //Generate the data
    val df = sc.parallelize((1 to 1000).map(i => Record(i,s"val_$i"))).toDF()

    // Any RDD containing case classes can be registered as a table.  The schema of the table is
    // automatically inferred using scala reflection.
    df.registerTempTable("records")

    // Now the table has been registered, you can run SQL queries over them.
    println("Result of SELECT *")
    sqlContext.sql("SELECT * FROM records").collect().foreach(println)

    //run the aggregation queries
    val count =   sqlContext.sql("SELECT count(*) FROM records ").collect().head.getLong(0)
    println(s"COUNT(*): $count")
    //stop the context

    // The results of SQL queries are themselves RDDs and support all normal RDD functions.  The
    // items in the RDD are of type Row, which allows you to access each column by ordinal.
    val rddFromSQL = sqlContext.sql("SELECT key, value FROM records where key < 10")
    println("Result of RDD.map")
    rddFromSQL.map(row => s"Key: ${row(0)}, Value: ${row(1)}").collect().foreach(println)

    // Queries can also be written using a LINQ-like Scala DSL.
    df.where($"key" === 10).orderBy($"value".asc).select($"key", $"value").collect().foreach(println)

    //write out a RDD as parquet file
    df.write.parquet("pair.parquet")

    //Read the parquet file.Parquet files are self-describing so the schema is preserved
    val parquetFile = sqlContext.read.parquet("pair.parquet")

    //Queries can be run using DSL on parquet file just like the original RDD
    parquetFile.where($"key" < 10).select($"key", $"value").collect().foreach(println)

    // These files can also be registered as tables.
    parquetFile.registerTempTable("parquetFile")
    sqlContext.sql("SELECT * FROM parquetFile").collect().foreach(println)

    //Stop the spark Context
    sc.stop()
  }

}
