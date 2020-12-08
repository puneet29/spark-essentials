package part2dataframes

import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc_nulls_last, expr}

object Joins extends App {
  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandsDF.show()

  // outer joins
  // left outer
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show()
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show()
  guitaristsDF.join(bandsDF, joinCondition, "outer").show()

  // semi joins (Only columns from left table for which there is data in right table)
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti join (Only columns from left table for which there is NO data in right table)
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show()

  // IMPORTANT Things to bear in mind while joining

  // guitaristsBandsDF.select("id", "band").show() // This will crash
  // There are two columns with name id in this join
  // Solution:
  // 1. Rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // 2. Drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // 3. Rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // Using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  // Exercise
  def readTable(tableName: String): sql.DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.${tableName}")
    .load()

  val employees = readTable("employees")
  val salaries = readTable("salaries")
  val deptManagers = readTable("dept_manager")
  val titles = readTable("titles")

  val maxSalariesPerEmp = salaries.groupBy("emp_no").max("salary")
  val employeesMaxSalaries = employees.join(maxSalariesPerEmp, "emp_no")
    .drop(salaries.col("emp_no"))
  employeesMaxSalaries.show()

  val employeesNotManagers = employees.join(deptManagers, employees.col("emp_no") === deptManagers.col("emp_no"), "left_anti")
    .drop(deptManagers.col("emp_no"))
  employeesNotManagers.show()

  val titlesOfTop10Employees = employees.join(salaries, "emp_no")
    .drop(salaries.col("emp_no"))
    .drop("from_date", "to_date")
    .orderBy(desc_nulls_last("salary"))
    .limit(10)
    .join(titles, "emp_no")
    .drop(titles.col("emp_no"))
  titlesOfTop10Employees.show()
}
