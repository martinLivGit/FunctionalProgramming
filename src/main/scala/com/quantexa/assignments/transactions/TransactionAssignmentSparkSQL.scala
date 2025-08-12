/*
  Quantexa Copyright Statement
 */

package com.quantexa.assignments.transactions

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

/***
  * This Object holds the functions required for the Quantexa coding exercise and an entry point to execute them.
  * Once executed each question is executed in turn printing results to console
  */

/***
  * A case class to represent a transaction
  * @param transactionId The transaction Identification String
  * @param accountId The account Identification String
  * @param transactionDay The day of the transaction
  * @param category The category of the transaction
  * @param transactionAmount The transaction value
  */

object TransactionAssignmentSparkSQL {

  def apply( transactions: List[Transaction]) : List[DayAccountStats] = {

     val spark: SparkSession = sparkInit(transactions)

     sumTransactions(spark)
     avgCategoryTransactions(spark)
     createSliding5dayTransactions(spark)
     createsliding5daySumAndAvg(spark)
     transactionWithin5DaysCategorySums(spark)
     finalResult(spark)
  }

   private def finalResult(spark: SparkSession) = {
      println("Exercise 3_3 =>")
      val ex3_3 = spark.sql(
         """
             SELECT
                   A.slidingTransactionDay as transactionDay
                   ,A.accountId
                   ,NVL(A.maxTransactionAmount,0) as maxTransaction
                   ,NVL(A.avgTransactionAmount,0) as avgTransaction
                   ,COALESCE(AA,0) as aaTotal
                   ,COALESCE(CC,0) as ccTotal
                   ,COALESCE(FF,0) as ffTotal
             FROM transactionWithin5DaysSumAndAvg A
             FULL OUTER JOIN transactionWithin5DaysCategorySums B
             ON A.accountId = B.accountId
             AND A.slidingTransactionDay = B.slidingTransactionDay
             AND A.accountId IS NOT NULL
             ORDER BY A.accountId, A.slidingTransactionDay
            """)

      ex3_3.createTempView("finalResult")
      ex3_3.show(100)
      ex3_3.printSchema()

      ex3_3.as[DayAccountStats].collect().toList
   }

   private def transactionWithin5DaysCategorySums(spark: SparkSession) = {
      println("Exercise 3_2 =>")
      val ex3_2 = spark.sql(
         """
            SELECT *
            FROM
              ( SELECT slidingTransactionDay
                     ,accountId
                     ,category
                     ,transactionAmount
                FROM transactionWithin5Days
                WHERE category in ('AA','CC','FF')
              )
             PIVOT
              (
                COALESCE(ROUND(SUM(transactionAmount),2),0) AS amt
                FOR category IN ('AA','CC','FF')
              )
              ORDER BY accountId, slidingTransactionDay
         """)

      ex3_2.createTempView("transactionWithin5DaysCategorySums")
      ex3_2.show(100)
      ex3_2.printSchema()
   }

   private def createsliding5daySumAndAvg(spark: SparkSession) = {
      println("Exercise 3_1 =>")
      val ex3_1 = spark.sql(
         """
            SELECT slidingTransactionDay
                   ,accountId
                   ,MAX(transactionAmount) as maxTransactionAmount
                   ,ROUND(AVG(transactionAmount),2) as avgTransactionAmount
            FROM transactionWithin5Days
            GROUP BY accountId, slidingTransactionDay
            ORDER BY accountId, slidingTransactionDay
          """)

      ex3_1.createTempView("transactionWithin5DaysSumAndAvg")
      ex3_1.show(100)
      ex3_1.printSchema()
   }

   private def createSliding5dayTransactions(spark: SparkSession) = {
      println("Exercise 3 =>")
      println("Exercise 3_0 =>")
      val ex3_0 = spark.sql(
         """
            SELECT *
            FROM transactions A
            RIGHT OUTER JOIN (
              SELECT distinct transactionDay as slidingTransactionDay
              FROM transactions
             ) B
            ON A.transactionDay >= slidingTransactionDay-5
            AND A.transactionDay < slidingTransactionDay
            ORDER BY accountId, slidingTransactionDay, A.transactionDay, transactionId
          """)

      ex3_0.createTempView("transactionWithin5Days")
      ex3_0.show(100)
      ex3_0.printSchema()
   }

   private def avgCategoryTransactions(spark: SparkSession) = {
      println("Exercise 2 =>")
      val ex2 = spark.sql(
         """
             SELECT *
             FROM
               (SELECT accountId, category, transactionAmount
               FROM transactions)
             PIVOT (
                 ROUND(AVG(transactionAmount),2) AS amt
                 FOR category IN ('AA' as AA_AVG,'BB' as BB_AVG,'CC' as CC_AVG,'DD' as DD_AVG,'EE' as EE_AVG,'FF' as FF_AVG,'GG' as GG_AVG)
             )
             ORDER BY accountId
            """)

      ex2.show(100)
      ex2.printSchema()
   }

   private def sumTransactions(spark: SparkSession) = {
      println("Exercise 1 =>")
      val ex1 = spark.sql(
         """
             SELECT transactionDay, ROUND(SUM(transactionAmount),2) transactionSum
             FROM transactions
             GROUP BY transactionDay
             ORDER BY transactionDay
             """).persist()

      ex1.printSchema()
      ex1.show(100)
   }

   private def sparkInit(transactions: List[Transaction]) = {
      //Create a spark context, using a local master so Spark runs on the local machine
      val spark = SparkSession.builder().master("local[*]").appName("AccountAssignment").getOrCreate()

      //importing spark implicits allows functions such as dataframe.as[T]
      import spark.implicits._

      //Set logger level to Warn
      Logger.getRootLogger.setLevel(Level.WARN)
      val ds: Dataset[Transaction] = transactions.toDS()
      ds.createTempView("transactions")
      spark
   }

}

