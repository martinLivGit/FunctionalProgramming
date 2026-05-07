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

  private val aggOp : ( (Double, Double, Double, Double, Double), (Int, Transaction) ) => (Double, Double, Double, Double, Double) =
  {
    case ( (m:Double,t:Double,aa:Double,cc:Double,ff:Double), (_,txn:Transaction) ) =>
      ( if (txn.transactionAmount > m) txn.transactionAmount else m
        ,t + txn.transactionAmount
        ,if (txn.category == "AA") aa + txn.transactionAmount else aa
        ,if (txn.category == "CC") cc + txn.transactionAmount else cc
        ,if (txn.category == "FF") ff + txn.transactionAmount else ff )
  }

  def txnAggregatorWithFold( dyAccTxnList:((Int,String), IndexedSeq[(Int,Transaction)]) ): DayAccountStats = {
    dyAccTxnList match {
      case ((day: Int, account: String), dyTxns: IndexedSeq[(Int, Transaction)]) =>
        val (max, tot, aa, cc, ff) = dyTxns.foldLeft((0D, 0D, 0D, 0D, 0D))(aggOp)
        DayAccountStats(day, account, max, tot / dyTxns.size, aa, cc, ff)
    }
  }

  def txnAggregatorWithFilter( dyAccTxns:((Int,String), IndexedSeq[(Int,Transaction)]) ) : DayAccountStats = {
    val txnTotalAmount = ( category: String) => dyAccTxns._2.filter(_._2.category == category).map(_._2.transactionAmount).sum
    val txnAmount = dyAccTxns._2.map(_._2.transactionAmount)

    val tot = txnAmount.sum
    val max = txnAmount.max
    val avg = tot/dyAccTxns._2.size
    val aa = txnTotalAmount("AA")
    val cc = txnTotalAmount("CC")
    val ff = txnTotalAmount("FF")

    DayAccountStats(dyAccTxns._1._1,dyAccTxns._1._2,max,avg,aa,cc,ff)
  }

  def apply(transactions: List[Transaction]
            ,aggregator: (((Int, String), IndexedSeq[(Int, Transaction)])) => DayAccountStats = txnAggregatorWithFilter): List[DayAccountStats] =
  {
     (for {
       reportDay <- 1 to 31
       txn <- transactions if txn.transactionDay >= reportDay-5 && txn.transactionDay < reportDay
      } yield (reportDay, txn) )
     .groupBy( p => (p._1, p._2.accountId))
     .map(aggregator)
     .toList
   }
}
