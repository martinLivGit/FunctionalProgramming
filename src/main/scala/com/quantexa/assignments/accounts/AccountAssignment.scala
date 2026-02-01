package com.quantexa.assignments.accounts

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.:+

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

case class CustomerAccountData(
   customerId: String,
   forename: String,
   surname: String,
   accountId: String,
   balance: Long
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

   private def customerAccountsAggregatorWithFold (customerId: String, acctIt: Iterator[CustomerAccountData]) : CustomerAccountOutput = {
      val acctLst = acctIt.toList
      val (accounts, totalBalance) = acctLst.foldLeft((List[AccountData](), 0L)) {
        case ( (l:List[AccountData], t:Long), acct: CustomerAccountData) =>
          (  l :+ AccountData(customerId, acct.accountId, acct.balance), t + acct.balance)
      }
      CustomerAccountOutput(customerId, acctLst.head.forename, acctLst.head.surname, accounts, acctLst.size, totalBalance, totalBalance/acctLst.size )
  }

  private def customerAccountsAggregator (customerId: String, acctIt: Iterator[CustomerAccountData]) : CustomerAccountOutput = {
    val acctLst = acctIt.toList
    val forename = acctLst.head.forename
    val surname = acctLst.head.surname
    val totalBalance = acctLst.map(_.balance).sum
    val numberAccounts = acctLst.size
    val averageBalance = totalBalance/numberAccounts
    val accounts: List[AccountData] = acctLst.foldLeft(List[AccountData]()) {
      (accounts: List[AccountData], acct: CustomerAccountData) =>
        accounts :+ AccountData(customerId, acct.accountId, acct.balance)
     }
    CustomerAccountOutput(customerId,forename,surname,accounts,numberAccounts, totalBalance, averageBalance)
  }

   def apply(customersDS: Dataset[CustomerData], accountsDS: Dataset[AccountData]): List[CustomerAccountOutput] = {

      //join customer data with account
      //drop the second customerId column on account
      //Group accounts by customer id
      //map the group to the required custom aggregate by folding the elements of each group
      //return result as a list
      customersDS
         .join(accountsDS
            ,customersDS.col("customerId").equalTo(accountsDS.col("customerId"))
            ,"full_outer")
         .drop(customersDS.col("customerId"))
         .na.fill((Map("balance" -> 0, "forename" -> "", "surname" -> "")))
         .as[CustomerAccountData]
         .groupByKey(_.customerId)
         .mapGroups(customerAccountsAggregator)
         .collect()
         .toList
   }
}

