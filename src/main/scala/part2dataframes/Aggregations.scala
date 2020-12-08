package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, desc_nulls_last, max, mean, min, stddev, sum}

object Aggregations extends App {
  val spark = SparkSession.builder()
    .appName("Aggregations and Grouping")
    .config("spark.master", "local")
    .getOrCreate()

  // Aggregations and groupings are wide transformations

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values expect null
  moviesDF.selectExpr("count(Major_Genre)") // equivalent

  moviesDF.select(count("*")) // Will count all the rows including nulls

  // counting distinct
  moviesDF.select(countDistinct(col("Major_Genre")))

  // approximate count (Useful for big data analysis)
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  moviesDF.selectExpr("min(IMDB_Rating)")
  val maxRatingDF = moviesDF.select(max(col("IMDB_Rating")))
  moviesDF.selectExpr("max(IMDB_Rating)")

  // sum
  moviesDF.select(sum(col("US_Gross")))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenre = moviesDF.
    groupBy(col("Major_Genre")).
    count()
  //  countByGenre.show()

  val avgRatingByGenreDF = moviesDF.
    groupBy(col("Major_Genre")).
    avg("IMDB_Rating")
  //  avgRatingByGenreDF.show()

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("n_movies"),
      avg("IMDB_Rating").as("avg_rating")
    ).orderBy(col("avg_rating"))
  aggregationsByGenreDF.show()

  // Exercises
  val totProfitsDF = moviesDF
    .groupBy(col("Title"))
    .agg(
      sum("US_Gross").as("Total_US_Gross"),
      sum("Worldwide_Gross").as("Total_Worldwide_Gross")
    ).withColumn("Total Profit", col("Total_US_Gross") + col("Total_Worldwide_Gross"))
  totProfitsDF.show()

  val countDistinctDirectorsDF = moviesDF
    .select(countDistinct("Director"))
  countDistinctDirectorsDF.show()

  val meanAndStddevDF = moviesDF
    .select(
      mean("US_Gross"),
      stddev("US_Gross")
    )
  meanAndStddevDF.show()

  val directorRatingAndProfitDF = moviesDF
    .groupBy(col("Director"))
    .agg(
      avg("IMDB_Rating").as("avg_rating"),
      sum("US_Gross").as("total_gross")
    ).orderBy(desc_nulls_last("avg_rating"), desc_nulls_last("total_gross"))
  directorRatingAndProfitDF.show()
}
