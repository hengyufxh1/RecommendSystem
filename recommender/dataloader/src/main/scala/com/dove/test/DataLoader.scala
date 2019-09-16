package com.dove.test

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 功能描述 ：DataLoader
  *
  * 加载数据
  * 流程 spark 读取文件写入大mongodb es
  *
  * @author ：smart-dxw
  * @version ：2019/9/16 21:07 v1.0
  */
object DataLoader {
  // MongoDB中的表 collection
  // Moive在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION = "Movie"

  // Rating在MongoDB中的Collection名称【表】
  val RATING_COLLECTION = "Rating"

  // Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION = "Tag"




  def main(args: Array[String]): Unit = {

    // 声明一些常量
    // 声明数据的路径
    val DATAFILE_MOVIES = "E:\\bigdata\\hadoop\\RecommendSystem\\reco_data\\small\\movies.csv"
    val DATAFILE_RATINGS = "E:\\bigdata\\hadoop\\RecommendSystem\\reco_data\\small\\ratings.csv"
    val DATAFILE_TAGS = "E:\\bigdata\\hadoop\\RecommendSystem\\reco_data\\small\\tags.csv"


    // 创建一个全局配置
    val param = scala.collection.mutable.Map[String, Any]()
    param += "spark.cores" -> "local[2]"
    param += "mongo.uri" -> "mongodb://11.11.11.151:27017/recom"
    param += "mongo.db" -> "recom"

    implicit val mongoConf = new MongoConfig(param("mongo.uri").asInstanceOf[String], param("mongo.db").asInstanceOf[String])

    // 声明 spark 环境
    val config = new SparkConf().setAppName("DataLoader").setMaster(param("spark.cores").asInstanceOf[String])
    val spark = SparkSession.builder().config(config).getOrCreate()

    // 加载数据集
    val movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)
    val ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    val tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    // 不引入toDF() show() 会报错
    import spark.implicits._
    // 把RDD转换成DataFrame
    val movieDF = movieRDD.map(line => {
      val x = line.split("\\^")
      Movie(x(0).trim.toInt, x(1).trim, x(2).trim, x(3).trim, x(4).trim, x(5).trim, x(6).trim, x(7).trim, x(8).trim, x(9).trim)
    }).toDF()


    val ratingDF = ratingRDD.map(line => {
      val x = line.split(",")
      Rating(x(0).trim.toInt, x(1).trim.toInt, x(2).trim.toDouble, x(3).trim.toInt)
    }).toDF()


    val tagDF = tagRDD.map(line => {
      val x = line.split(",")
      Tag(x(0).trim.toInt, x(1).trim.toInt, x(2).trim, x(3).trim.toInt)
    }).toDF()

//    movieDF.show()
//    ratingDF.show()
//    tagDF.show()




    // 把数据保存到mongoDB
//        storeDataInMongo(movieDF,ratingDF,tagDF)

    movieDF.cache()
    tagDF.cache()
    // 处理数据

    import org.apache.spark.sql.functions._
    // 引入内置函数库  agg聚合函数
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags"))

    // 将tags合并到movie表，产生新的movie数据集
    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),"left")
      .select("mid","name","descri","timelong","issue","shoot","language","genres","actors","directors","tags")

    esMovieDF.show()
    // 把数据保存到ES
//    storeDataInES(esMovieDF)
  }

  /**
    * 将数据保存到MongoDB
    *
    * @param movieDF
    * @param ratingDF
    * @param tagDF
    */
  def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit config: MongoConfig): Unit = {

    // 创建到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(config.uri))

    mongoClient(config.db)(MOVIES_COLLECTION).dropCollection()
    mongoClient(config.db)(RATING_COLLECTION).dropCollection()
    mongoClient(config.db)(TAGS_COLLECTION).dropCollection()

    // 把数据写入到MongoDB
    movieDF.write.option("uri", config.uri).option("collection", MOVIES_COLLECTION).format("com.mongodb.spark.sql").save()
    ratingDF.write.option("uri", config.uri).option("collection", RATING_COLLECTION).format("com.mongodb.spark.sql").save()
    tagDF.write.option("uri", config.uri).option("collection", TAGS_COLLECTION).format("com.mongodb.spark.sql").save()

    mongoClient(config.db)(MOVIES_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient(config.db)(RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config.db)(RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient(config.db)(TAGS_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(config.db)(TAGS_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()

  }

  def storeDataInES(esMovieDF: DataFrame): Unit = ???
}
