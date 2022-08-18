package com.vijay.spark.scoringModel

import com.vijay.spark.accounts.AccountAssessment.{AccountData, CustomerAccountOutput}
import com.vijay.spark.customerAddresses.CustomerAddress.{AddressData, spark}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{array_contains, col}

object ScoringModel extends App {

  val CountryToLookUp = "British Virgin Islands"
  val ColCountryAddress = "address.country"

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("ScoringModel").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]
  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )

  case class ScoringModel(
                           customerId: String,
                           forename: String,
                           surname: String,
                           //Accounts for this customer
                           accounts: Seq[AccountData],
                           //Addresses for this customer
                           address: Seq[AddressData],
                           linkToBVI: Boolean
                         )
  val customerAddressDS = spark.read.parquet("src/main/resources/customerAddressOutputDS.parquet").as[CustomerDocument]

  val result = customerAddressDS
    .withColumn("linkToBVI", array_contains(col(ColCountryAddress),  CountryToLookUp))
    .filter(array_contains(col(ColCountryAddress),  CountryToLookUp))
    .as[ScoringModel]

  result.show(truncate = false)
  result.write.mode(SaveMode.Overwrite).parquet("src/main/resources/scoringModelOutputDS.parquet")

  //END GIVEN CODE
}
