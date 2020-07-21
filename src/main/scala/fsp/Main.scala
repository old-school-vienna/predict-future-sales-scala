package fsp

import java.nio.file.Path
import java.text.ParsePosition
import java.time.{Duration, LocalDateTime}
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.util.{Calendar, Date, Locale, TimeZone}

import org.apache.commons.lang3.time.DateParser
import org.apache.commons.math3.transform.DftNormalization
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion.DateTimeOperations
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}

/*
drwxrwxr-x 2 wwagner4 wwagner4      4096 Jul 19 13:00 ./
drwxrwxr-x 4 wwagner4 wwagner4      4096 Jul 14 18:48 ../
-rw-rw-r-- 1 wwagner4 wwagner4  15845085 Jul 14 18:56 competitive-data-science-predict-future-sales.zip
-rw-rw-r-- 1 wwagner4 wwagner4      3573 Dec 15  2019 item_categories.csv
-rw-rw-r-- 1 wwagner4 wwagner4   1568417 Dec 15  2019 items.csv
-rw-rw-r-- 1 wwagner4 wwagner4  94603866 Dec 15  2019 sales_train.csv
-rw-r--r-- 1 root     root     128366283 Jul 14 21:24 sales_train_dn.csv
-rw-rw-r-- 1 wwagner4 wwagner4   2245108 Dec 15  2019 sample_submission.csv
-rw-rw-r-- 1 wwagner4 wwagner4      2977 Dec 15  2019 shops.csv
-rw-rw-r-- 1 wwagner4 wwagner4   1925720 Jul 19 11:32 Test2_MeanMonths.csv
-rw-rw-r-- 1 wwagner4 wwagner4   3182735 Dec 15  2019 test.csv
-rw-rw-r-- 1 wwagner4 wwagner4 114876857 Jul 19 13:00 TrainMonthly.csv
(base) wwagner4@ben:/data/kaggle/pfs$ 

date,date_block_num,shop_id,item_id,item_price,item_cnt_day
02.01.2013,0,59,22154,999.0,1.0
03.01.2013,0,25,2552,899.0,1.0
05.01.2013,0,25,2552,899.0,-1.0
06.01.2013,0,25,2554,1709.05,1.0

StructType(StructField(date,StringType,true), StructField(date_block_num,IntegerType,true), StructField(shop_id,IntegerType,true), 
StructField(item_id,IntegerType,true), StructField(item_price,DoubleType,true), StructField(item_cnt_day,DoubleType,true))

map struct<
date:string,
date_block_num:string,
shop_id:string,
item_id:string,
item_price:string,
item_cnt_day:string>
 */


object Main {

  case class DsTrain(
                      date: String,
                      dateBlockNum: Int,
                      shopId: Int,
                      itemId: Int,
                      itemPrice: Double,
                      itemCntDay: Double,
                    )

  case class DsTl(
                   date: Int,
                   itemCntDay: Double,
                 )

  case class K1(
                 itemId: Int,
                 date: String,
               )

  case class K2(
                 itemId: Int,
                 shopId: Int,
               )

  def toDsTrain(r: Row): DsTrain = {
    DsTrain(
      r.getString(0),
      r.getString(1).toInt,
      r.getString(2).toInt,
      r.getString(3).toInt,
      r.getString(4).toDouble,
      r.getString(4).toDouble,
    )
  }

  private lazy val dfmt = DateTimeFormatter.ofPattern("dd.mm.yyyy")
  private lazy val startDay = LocalDateTime.parse("01.01.2013", dfmt)


  def toIntDate(d: String): Int = {
    val ld = LocalDateTime.parse(d, dfmt)
    Duration.between(startDay, ld).toDays.toInt
  }

  implicit val orienc: Encoder[DsTrain] = Encoders.kryo[DsTrain]

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("main").master("local").getOrCreate()

    val dd = System.getenv("DATADIR")
    val infilePath = Path.of(dd, "pfs", "sales_train.csv")
    println(s"-- infilePath $infilePath")
    val dsTrain: RDD[DsTrain] = spark.read
      .options(Map("delimiter" -> ",", "header" -> "true"))
      .csv(infilePath.toString)
      .map(toDsTrain)
      .rdd

    val grouped = dsTrain
      .groupBy(x => K2(x.itemId, x.shopId))
      .sortBy { case (k, _) => k.itemId }
      .sortBy { case (k, _) => k.shopId }
      .map { case (k, vals) => (k, vals.map(df => DsTl(toIntDate(df.date), df.itemCntDay)).toSeq.sortBy(x => x.date)) }
      .take(40).toList
    println(grouped.mkString("\n"))

    spark.stop()

  }

}