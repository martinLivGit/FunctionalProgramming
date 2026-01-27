package com.quantexa.assignments.transactions

case class Transaction(
  transactionId: String,
  accountId: String,
  transactionDay: Int,
  category: String,
  transactionAmount: Double
)

case class DayTotalValue(
  transactionDay: Int,
  total: Double
)

case class AvgValueForAccountAndCategory(
  accountId: String,
  category: String,
  averageValue: Double
)

case class DayAccountStats(
  transactionDay: Int,
  accountId: String,
  maxTransaction: Double,
  avgTransaction: Double,
  aaTotal: Double,
  ccTotal: Double,
  ffTotal: Double
)

case class DyAcctAccmltr(
  maxTransaction: Double
  ,aaTotal: Double
  ,ccTotal: Double
  ,ffTotal: Double
  ,total: Double
)

object TransactionAssignment {

  def txnAggregatorWithFold: ( Int, String, List[(Int, Transaction)]) => DayAccountStats =
    (day:Int, account: String, dyTxnLst: List[(Int,Transaction)]) =>
    {
      val agg = dyTxnLst.foldLeft(DyAcctAccmltr(0,0,0,0,0)) {
          case (acc: DyAcctAccmltr, (_, txn: Transaction)) =>
            val max = if (txn.transactionAmount > acc.maxTransaction) txn.transactionAmount else acc.maxTransaction
            txn.category match {
              case "AA" => DyAcctAccmltr(max, acc.aaTotal + txn.transactionAmount, acc.ccTotal, acc.ffTotal, acc.total + txn.transactionAmount)
              case "CC" => DyAcctAccmltr(max, acc.aaTotal , acc.ccTotal + txn.transactionAmount, acc.ffTotal, acc.total + txn.transactionAmount)
              case "FF" => DyAcctAccmltr(max, acc.aaTotal, acc.ccTotal, acc.ffTotal + txn.transactionAmount, acc.total + txn.transactionAmount)
              case _    => DyAcctAccmltr(max, acc.aaTotal, acc.ccTotal, acc.ffTotal, acc.total + txn.transactionAmount)
            }
        }
      DayAccountStats(day, account, agg.maxTransaction, agg.total/dyTxnLst.size,agg.aaTotal, agg.ccTotal, agg.ffTotal)
    }

    def txnAggregatorWithFilter: ( Int, String, List[(Int, Transaction)]) => DayAccountStats =
         (day:Int, account: String, dyTxnLst: List[(Int,Transaction)]) =>
   {
     val tot = dyTxnLst.map(_._2.transactionAmount).sum
     val max = dyTxnLst.map(_._2.transactionAmount).max
     val avg = tot/dyTxnLst.size
     val aa = dyTxnLst.filter(_._2.category == "AA").map(_._2.transactionAmount).sum
     val cc = dyTxnLst.filter(_._2.category == "CC").map(_._2.transactionAmount).sum
     val ff = dyTxnLst.filter(_._2.category == "FF").map(_._2.transactionAmount).sum

     DayAccountStats(day,account,max,avg,aa,cc,ff)
   }

   def apply(transactions: List[Transaction]
             ,aggregator: (Int, String, List[(Int, Transaction)]) => DayAccountStats = txnAggregatorWithFilter): List[DayAccountStats] =
   {
      (1 to 31)
         .flatMap( day =>  transactions.filter(txn => txn.transactionDay >= day-5 && txn.transactionDay < day)
                                       .map( txn => (day,txn)))
         .groupBy( p => (p._1, p._2.accountId))
         .map{ case (( day:Int, account:String), dyTxns) => (aggregator(day, account, dyTxns.toList))}
         .toList
   }
}
