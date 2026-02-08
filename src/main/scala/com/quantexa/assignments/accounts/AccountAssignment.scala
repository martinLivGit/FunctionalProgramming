package com.quantexa.assignments.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.:+
import scala.sys.exit

/***
 * A common problem we face at Quantexa is having lots of disjointed raw sources of data and having to aggregate and collect
 * relevant pieces of information into hierarchical case classes which we refer to as Documents. This exercise simplifies
 * the realities of this with just two sources of high quality data however reflects the types of transformations we must
 * perform.
 *
 * You have been given customerData and accountData. This has been read into a DataFrame for you and then converted into a
 * Dataset of the given case classes.
 *
 * If you run this App you will see the top 20 lines of the Datasets provided printed to console
 *
 * This allows you to use the Dataset API which includes .map/.groupByKey/.joinWith ect.
 * But a Dataset also includes all of the DataFrame functionality allowing you to use .join ("left-outer","inner" ect)
 *
 * https://spark.apache.org/docs/latest/sql-programming-guide.html
 *
 * The challenge here is to group, aggregate, join and map the customer and account data given into the hierarchical case class
 * customerAccountoutput. We would prefer you to write using the Scala functions instead of spark.sql() if you choose to perform
 * Dataframe transformations. We also prefer the use of the Datasets API.
 *
 * Example Answer Format:
 *
 * val customerAccountOutputDS: Dataset[customerAccountOutput] = ???
 * customerAccountOutputDS.show(1000,truncate = false)
 *
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |customerId|forename   |surname   |accounts                                                             |numberAccounts|totalBalance|averageBalance   |
 * +----------+-----------+----------+---------------------------------------------------------------------+--------------+------------+-----------------+
 * |IND0001   |Christopher|Black     |[]                                                                   |0             |0           |0.0              |
 * |IND0002   |Madeleine  |Kerr      |[[IND0002,ACC0155,323], [IND0002,ACC0262,60]]                        |2             |383         |191.5            |
 * |IND0003   |Sarah      |Skinner   |[[IND0003,ACC0235,631], [IND0003,ACC0486,400], [IND0003,ACC0540,53]] |3             |1084        |361.3333333333333|
 * ...
 */

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

object AccountAssignment {

   //Create a spark context, using a local master so Spark runs on the local machine
   val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("AccountAssignment")
      .getOrCreate()

   //importing spark implicits allows functions such as dataframe.as[T]
   import spark.implicits._

   //set logging level
   Logger.getRootLogger.setLevel(Level.WARN)

   private def customerAccountsAggregator(customerId: String, acctIt: Iterator[(String,(CustomerData,AccountData))]) : CustomerAccountOutput = {
      //fold the list for the given customerId - accumulating the total balance and list of accounts
      //return the summary of the customer info, list of accounts, and stats
      val acctLst = acctIt.toList
      val (accounts, totalBalance) = acctLst.foldLeft((List[AccountData](),0L)) {
         case ((l:List[AccountData],t:Long),(_,(_,account:AccountData))) if account != null => (l :+ account, t + account.balance)
         case ((l:List[AccountData],t:Long),_) => (l,t)       
      }
      val avgBalance = if ( accounts.isEmpty) 0 else totalBalance/accounts.size
      val (forename, surname) = if (acctLst.head._2._1 != null) (acctLst.head._2._1.forename, acctLst.head._2._1.surname) else ("","")
      CustomerAccountOutput(customerId,forename,surname,accounts,accounts.size,totalBalance,avgBalance)
   }

   def apply(customersDF: DataFrame, accountsDF: DataFrame): List[CustomerAccountOutput] = {

      val customersDS = customersDF.as[CustomerData]
      val accountsDS = accountsDF.withColumn("balance", 'balance.cast("long")).as[AccountData]

      //join customer data with account
      //add a customerId field populated from either customer or account objects
      //Group accounts by customer id
      //map the group to the required custom aggregate by folding the elements of each group
      //return result as a list
      customersDS
         .joinWith(accountsDS
            ,customersDS.col("customerId").equalTo(accountsDS.col("customerId"))
            ,"full_outer")
         .map( x => if (x._1 == null) (x._2.customerId,x) else (x._1.customerId,x))
         .groupByKey( x => x._1 )
         .mapGroups(customerAccountsAggregator)
         .collect()
         .toList
   }
}

