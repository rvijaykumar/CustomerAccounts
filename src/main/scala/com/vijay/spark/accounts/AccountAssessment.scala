package com.vijay.spark.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType

object AccountAssessment extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  //Create DataFrames of sources
  val customerDF: DataFrame = spark.read.option("header","true")
    .csv("src/main/resources/customer_data.csv")
  val accountDF = spark.read.option("header","true")
    .csv("src/main/resources/account_data.csv")

  case class CustomerData(
                           customerId: String,
                           forename: String,
                           surname: String
                         )

  case class AccountData(
                          customerId: String,
                          accountId: String,
                          balance: Long
                        )

  //Expected Output Format
  case class CustomerAccountOutput(
                                    customerId: String,
                                    forename: String,
                                    surname: String,
                                    //Accounts for this customer
                                    accounts: Seq[AccountData],
                                    //Statistics of the accounts
                                    numberAccounts: Int,
                                    totalBalance: Long,
                                    averageBalance: Double
                                  )

  //Create Datasets of sources
  val customerDS: Dataset[CustomerData] = customerDF.as[CustomerData]
  val accountDS: Dataset[AccountData] = accountDF.withColumn("balance",Symbol("balance").cast("long")).as[AccountData]

  //  customerDS.show
  //  accountDS.show

  val customerAccounts = accountDS.groupBy($"customerId")
    .agg(
      sum("balance").as("totalBalance"),
      avg("balance").as("averageBalance"),
      count("accountId").cast(IntegerType).as("numberAccounts"),
      collect_list(struct($"customerId", $"accountId", $"balance")).as("accounts"))

  val result = customerDS.join(customerAccounts, Seq("customerId"),"inner")
    .select("customerId", "forename", "surname", "accounts", "numberAccounts", "totalBalance", "averageBalance")
    .orderBy($"customerId")
    .as[CustomerAccountOutput]

  result.show(truncate = false)
  result.write.mode(SaveMode.Overwrite).parquet("src/main/resources/customerAccountOutputDS.parquet")
  //END GIVEN CODE
}
