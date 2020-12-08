package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // selecting (projecting) {Narrow Transformation}
  val carNameDF = carsDF.select(firstColumn)

  // --------------------------------------------------------------------
  // various select methods
  carsDF.select(
    carsDF.col("Name"),
    carsDF.col("Acceleration")
  )

  import spark.implicits._

  carsDF.select(
    col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto converted to column
    $"Horsepower", // fancier interpolated strings, returns a Column object
    expr("Origin") // Expression
  )

  // Select with plain column names
  carsDF.select("Name", "Acceleration")

  // Expressions
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // Select expression
  val carsWithSelectExprWeight = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )
  // --------------------------------------------------------------------

  // DF Processing
  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)
  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  // careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val americanCarsDF = carsDF.where(col("Origin") === "USA")
  // filtering with expression strings
  val americanCarsDF2 = carsDF.filter("Origin = 'USA'")
  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // Union-ing (Adding more rows)
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if DFs have the same schema

  // Distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  // Exercises
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  val projection1 = moviesDF.select("Title", "Release_Date", "IMDB_Rating")
  val projection2 = moviesDF.select((col("US_Gross") + col("Worldwide_Gross")).as("Total Profit"))
  val projection3 = moviesDF.filter("Major_Genre='Comedy' and IMDB_Rating > 6").select("Title")
  projection2.show()

}
