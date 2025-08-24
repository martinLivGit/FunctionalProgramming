package com.quantexa.assignments.accounts

//import org.scalatest.FunSuite
import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{FunSuite, Outcome, fixture}

class AccountAssignmentBaseSuite extends FunSuite {

   //Create a spark context, using a local master so Spark runs on the local machine
   val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AccountAssignment")
      .getOrCreate()

   //importing spark implicits allows functions such as dataframe.as[T]
   import spark.implicits._

   val customers: Seq[(String, String, String)] = Seq(("C00001","Bill", "Wright")
         ,("C00002","Bob", "Jones")
         ,("C00003","John", "Ball")
   )

   val accounts: Seq[(String, String, Int)] = Seq(
      ("C00001","A001", 50)
      ,("C00002","A002", 100)
      ,("C00003","A003", 200)
      ,("C00001","A004", 300)
      ,("C00003","A005", 400)
      ,("C00003","A006", 500)
   )

   val accountsDF: DataFrame = accounts.toDF("customerId", "accountId", "balance")
   val customersDF: DataFrame = customers.toDF( "customerId", "forename","surname")

   //Create Datasets of sources
   val customersDS: Dataset[CustomerData] = customersDF.as[CustomerData]
   val accountsDS: Dataset[AccountData] = accountsDF.withColumn("balance", 'balance.cast("long")).as[AccountData]

   val result: List[CustomerAccountOutput] = AccountAssignment(customersDS, accountsDS)

   test("results size") {
      assert( result.size == 3)
   }

   test("total results elements no.") {
      assert( result.map(p => p.accounts.size).sum == 6)
   }

   test("C00001 total balance") {
      assert( result
                  .filter(p => p.customerId == "C00001")
                  .head
                  .totalBalance
            == 350 )
   }

   test("Full customer C00001 result") {
      assert( result
         .filter(p => p.customerId == "C00001")
         .head
         == CustomerAccountOutput("C00001","Bill", "Wright"
		, List(AccountData("C00001","A001",50), AccountData("C00001","A004",300)), 2, 350, 175) )
   }

}
