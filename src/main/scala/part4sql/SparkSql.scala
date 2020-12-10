package part4sql

import org.apache.spark.sql
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.col
import part2dataframes.Joins.spark

object SparkSql extends App {
  // Specify Spark SQL Warehouse in config
  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Regular DF API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // Use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |SELECT Name FROM cars WHERE Origin = 'USA'
      |""".stripMargin
  )
  americanCarsDF.show()

  // We can run any SQL statement
  spark.sql("CREATE DATABASE rtjvm")
  spark.sql("USE rtjvm")
  val databaseDF = spark.sql("SHOW DATABASES")
  databaseDF.show()

  // Transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/rtjvm"
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String): sql.DataFrame = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.${tableName}")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false): Unit = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List("employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager"))

  // Read data from loaded spark tables
  val employeesDF2 = spark.read.table("employees")

  // Exercises
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")
  spark.sql("USE rtjvm")
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("moviesDF")

  spark.sql(
    """
      |SELECT COUNT(*) FROM employees
      |WHERE hire_date > '2000-01-01' and hire_date < '2001-01-01'
      |""".stripMargin).show()

  spark.sql(
    """
      |SELECT de.dept_no, avg(s.salary)
      |FROM employees e, dept_emp de, salaries s
      |WHERE e.hire_date > '2000-01-01' and e.hire_date < '2001-01-01'
      |AND e.emp_no = de.emp_no
      |AND e.emp_no = s.emp_no
      |GROUP BY de.dept_no
      |""".stripMargin).show()

  spark.sql(
    """
      |SELECT avg(s.salary) payments, d dept_name
      |FROM employees e, dept_emp de, salaries s, departments d
      |WHERE e.hire_date > '2000-01-01' and e.hire_date < '2001-01-01'
      |AND e.emp_no = de.emp_no
      |AND e.emp_no = s.emp_no
      |AND de.dept_no = d.dept_no
      |GROUP BY d.dept_name
      |ORDER BY payments desc
      |limit 1
      |""".stripMargin).show()
}
