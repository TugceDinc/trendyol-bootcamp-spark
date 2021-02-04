package com.trendyol.bootcamp.homework

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

import java.io.File
import scala.reflect.io.Directory
import scala.util.Try

object ProductMergerJob {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("product manager job")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    val productsSchema = Encoders.product[ProductBatch].schema

    val lastProcessedProducts = Try(
      spark.read
        .schema(productsSchema)
        .json("homework_output/batch/*")
        .as[ProductBatch]
    ).getOrElse(spark.emptyDataset[ProductBatch])

    if(lastProcessedProducts.isEmpty){
      val dir = new Directory(new File("homework_output/batch"))
      dir.createDirectory()
    }

    val productsCDC =
      if (lastProcessedProducts.isEmpty) {
        spark.read
          .schema(productsSchema)
          .json("data/homework/initial_data.json")
          .as[ProductBatch]
      }
      else
        spark.read
        .schema(productsSchema)
        .json("data/homework/cdc_data.json")
        .as[ProductBatch]

    productsCDC
      .union(lastProcessedProducts)
      .groupByKey(l => l.id)
      .reduceGroups((first, second) =>
        if(first.timestamp > second.timestamp) first
        else second
      ).show(false)

    val p = productsCDC
      .union(lastProcessedProducts)
      .groupByKey(l => l.id)
      .reduceGroups((first, second) =>
        if(first.timestamp > second.timestamp) first
        else second
      )
      .map{case(key,value) => value}

    p.show(false)

    p.write
      .format("json")
      .mode(SaveMode.Overwrite)
      .save("homework_output/batch_tmp")


    val dir = new Directory(new File("homework_output/batch"))
    dir.deleteRecursively()

    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    fs.rename(new Path("homework_output/batch_tmp"), new Path(s"homework_output/batch"))

  }

  case class ProductBatch(id:Int, name:String, category:String, brand:String, color:String, price:Double, timestamp:Long)
}