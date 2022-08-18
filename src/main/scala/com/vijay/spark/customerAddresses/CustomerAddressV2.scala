package com.vijay.spark.customerAddresses

import com.vijay.spark.accounts.AccountAssessment.{AccountData, CustomerAccountOutput}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}

object CustomerAddressV2 extends App {

  //Create a spark context, using a local master so Spark runs on the local machine
  val spark = SparkSession.builder().master("local[*]").appName("CustomerAddress").getOrCreate()

  //importing spark implicits allows functions such as dataframe.as[T]

  import spark.implicits._

  //Set logger level to Warn
  Logger.getRootLogger.setLevel(Level.WARN)

  case class AddressRawData(
                             addressId: String,
                             customerId: String,
                             address: String
                           )

  case class AddressData(
                          addressId: String,
                          customerId: String,
                          address: String,
                          number: Option[Int],
                          road: Option[String],
                          city: Option[String],
                          country: Option[String]
                        )

  //Expected Output Format
  case class CustomerDocument(
                               customerId: String,
                               forename: String,
                               surname: String,
                               //Accounts for this customer
                               accounts: Seq[AccountData],
                               //Addresses for this customer
                               address: Seq[AddressData]
                             )


  /**
   * Modified version of addressParser
   * @param unparsedAddress AddressData
   * @return AddressData
   */
  def addressParser(unparsedAddress: AddressData): AddressData = {
    val split = unparsedAddress.address.split(", ")

    unparsedAddress.copy(
        number = Some(split(0).toInt),
        road = Some(split(1)),
        city = Some(split(2)),
        country = Some(split(3))
      )
  }


  val addressDS = spark.read.option("header", "true")
    .csv("src/main/resources/address_data.csv")
    .withColumn("number", lit(0))
    .withColumn("road", lit(""))
    .withColumn("city", lit(""))
    .withColumn("country", lit(""))
    .as[AddressData]
    .map(addressData => addressParser(addressData))

  val customerAccountDS = spark.read.parquet("src/main/resources/customerAccountOutputDS.parquet").as[CustomerAccountOutput]

  val addressGroupedByCustomer = addressDS
    .groupBy($"customerId")
    .agg(collect_list(struct("*")).as("address"))

  val result = customerAccountDS.join(addressGroupedByCustomer, Seq("customerId"), "inner")
    .select("customerId", "forename", "surname", "accounts", "address")
    .as[CustomerDocument]

  result.show(truncate = false)
  result.write.mode(SaveMode.Overwrite).parquet("src/main/resources/customerAddressOutputDS.parquet")
  //END GIVEN CODE
}