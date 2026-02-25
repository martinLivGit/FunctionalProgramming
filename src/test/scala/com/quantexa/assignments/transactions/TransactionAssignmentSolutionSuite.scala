package com.quantexa.assignments.transactions

//import org.scalatest.FunSuite
import com.quantexa.assignments.transactions.TransactionAssignment.txnAggregatorWithFold
import org.scalatest.{FunSuite, fixture}

import scala.io.Source

class TransactionAssignmentSolutionSuite extends FunSuite {

  //The full path to the file to import
  val fileName : String = "C:/Users/martin/Documents/Dev/Project/Quantexa/Spark Project Template/SparkTestProjectWithAnswers/SparkTestProject/build/resources/test/transactions.csv";

  //The lines of the CSV file (dropping the first to remove the header)
  val transactions : Iterator[String]  = Source.fromFile(fileName).getLines().drop(1)

  val transactionsList : List[Transaction] = transactions.map {
    line =>
      val split = line.split(',')
      Transaction(split(0), split(1), split(2).toInt, split(3), split(4).toDouble)
  }.toList

  val dayAccountStats: List[DayAccountStats] = TransactionAssignment(transactionsList,txnAggregatorWithFold)

  test("dayAccountStats print") {
    println(dayAccountStats.sortBy(p=>(p.accountId,p.transactionDay)))
    assert( true )
  }

  test("dayAccountStats result checks") {
    assert( dayAccountStats.contains(DayAccountStats(4,"A11",914.89,597.085,1906.46,0.0,0.0)) )
    assert( dayAccountStats.contains(DayAccountStats(24,"A18",965.08,470.02000000000004,554.78,932.72,183.53)))
  }

  test("accounts stats") {
    assert( dayAccountStats.map(x=>x.accountId).toSet == Set("A9", "A22", "A47", "A33", "A11", "A4", "A10", "A15", "A32", "A26", "A21", "A37", "A46", "A41", "A12", "A5", "A16", "A49", "A38", "A27", "A31", "A34", "A23", "A19", "A20", "A39", "A45", "A17", "A28", "A2", "A13", "A40", "A42", "A1", "A6", "A30", "A35", "A24", "A36", "A29", "A43", "A18", "A8", "A3", "A14", "A44", "A7", "A25", "A48"))
    assert( dayAccountStats.size == 1369)
    assert( dayAccountStats.map(x=>x.accountId).distinct.size == 49 )
  }

  test("missing day records accounted for") {

    val accounts = dayAccountStats.map(x=>x.accountId).distinct
    val mapDayAccountStats = (
      for {
        day <- 1 to 31
        account <- accounts
        stat <- dayAccountStats if stat.transactionDay == day && stat.accountId == account
      } yield (account, day ) -> stat
    ).toMap

    val completeDayAccountStats = ( for {
      day <- 1 to 31
      account <- accounts
    } yield mapDayAccountStats.getOrElse((account,day),DayAccountStats(day,account,-1D,-1D,-1D,-1D,-1D))).toList

    assert(dayAccountStats.map(x=>x.accountId).distinct.size * 31 == completeDayAccountStats.size)
    
  }

}
