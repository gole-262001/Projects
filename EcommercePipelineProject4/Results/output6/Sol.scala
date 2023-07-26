
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sol {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Behaviour Analysis")
      .getOrCreate()

    val click = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/click.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val customer = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/customer.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val purchase = spark.read.option("header", true)
      .format("csv")
      .option("path", "input/purchase.csv")
      .option("maxFilesPerTrigger", 1)
      .load()

    val click_new = click.select(split(col("userID,timestamp,page"), ",").getItem(0).as("userID"),
      split(col("userID,timestamp,page"), ",").getItem(1).as("timestamp"),
      split(col("userID,timestamp,page"), ",").getItem(2).as("page"))
    click_new.show()

    val customer_new = customer.select(split(col("userID,name,email"), ",").getItem(0).as("userID"),
      split(col("userID,name,email"), ",").getItem(1).as("name"),
      split(col("userID,name,email"), ",").getItem(2).as("email"))
    customer.show()

    val purchase_new = purchase.select(split(col("userID,timestamp,amount"), ",").getItem(0).as("userID"),
      split(col("userID,timestamp,amount"), ",").getItem(1).as("timestamp"),
      split(col("userID,timestamp,amount"), ",").getItem(2).as("amount"))
    purchase.show()

    click_new.createOrReplaceTempView("table1")
    customer_new.createOrReplaceTempView("table2")
    purchase_new.createOrReplaceTempView("table3")


    val user_id = spark.sql("select count(distinct userid) from table2")
    user_id.show()
    val amount_sum = spark.sql("select sum(amount) from table3")
    amount_sum.show()
    val joining_data = spark.sql("select table2.name , max(table3.amount) from table2 join table3 on table2.userid = table3.userid group by(table2.name)")
    joining_data.show();

    click_new.write.csv("output/clickstream")
    customer_new.write.csv("output/customer")
    purchase_new.write.csv("output/purchase")
    user_id.write.csv("output/NumberOfUser")
    amount_sum.write.csv("output/TotalSum")
    joining_data.write.csv("output/FullInformation")


  }

}
