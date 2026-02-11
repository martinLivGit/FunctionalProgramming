package com.quantexa.assignments.transactions

case class Transaction(
  transactionId: String,
  accountId: String,
  transactionDay: Int,
  category: String,
  transactionAmount: Double
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

object TransactionAssignment {

  def txnAggregatorWithFold(day:Int, account: String, dyTxnLst: List[(Int,Transaction)]):DayAccountStats = {
      val (max,tot,aa,cc,ff) = dyTxnLst.foldLeft((0D,0D,0D,0D,0D))  {
          case ( (m: Double, t:Double, aa:Double, cc:Double, ff:Double), (day: Int, txn: Transaction)) =>
            val max = if (txn.transactionAmount > m) txn.transactionAmount else m
            txn.category match {
              case "AA" => (max, t + txn.transactionAmount, aa + txn.transactionAmount, cc, ff)
              case "CC" => (max, t + txn.transactionAmount, aa , cc + txn.transactionAmount, ff)
              case "FF" => (max, t + txn.transactionAmount, aa, cc, ff + txn.transactionAmount)
              case _    => (max, t + txn.transactionAmount, aa, cc, ff)
            }
      }
      DayAccountStats(day, account, max, tot/dyTxnLst.size, aa, cc, ff)
  }

  def txnAggregatorWithFilter( day:Int, account:String, dyTxnLst:List[(Int, Transaction)]) : DayAccountStats = {
      val txnTotalAmount = ( category: String) => dyTxnLst.filter(_._2.category == category).map(_._2.transactionAmount).sum

      val tot = dyTxnLst.map(_._2.transactionAmount).sum
      val max = dyTxnLst.map(_._2.transactionAmount).max
      val avg = tot/dyTxnLst.size
      val aa = txnTotalAmount("AA")
      val cc = txnTotalAmount("CC")
      val ff = txnTotalAmount("FF")

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
