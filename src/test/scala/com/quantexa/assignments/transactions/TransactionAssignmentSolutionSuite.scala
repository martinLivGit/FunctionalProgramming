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

  test("couple of dayAccountStats result tests") {
    assert( dayAccountStats.contains(DayAccountStats(4,"A11",914.89,597.085,1906.46,0.0,0.0)) )
    assert( dayAccountStats.contains(DayAccountStats(24,"A18",965.08,470.02000000000004,554.78,932.72,183.53)))
  }

  test("accounts stats") {
    println(dayAccountStats.map(x=>x.accountId).distinct.sorted)
    assert( dayAccountStats.map(x=>x.accountId).distinct.sorted == List("A1", "A10", "A11", "A12", "A13", "A14", "A15", "A16", "A17", "A18", "A19", "A2", "A20", "A21", "A22", "A23", "A24", "A25", "A26", "A27", "A28", "A29", "A3", "A30", "A31", "A32", "A33", "A34", "A35", "A36", "A37", "A38", "A39", "A4", "A40", "A41", "A42", "A43", "A44", "A45", "A46", "A47", "A48", "A49", "A5", "A6", "A7", "A8", "A9") )
    assert( dayAccountStats.size == 1369)
    assert( dayAccountStats.map(x=>x.accountId).distinct.size == 49 )
  }

  test("missing records accounted for") {    
    val mapDayAccountStats = (
      for {
        day <- 1 to 31
        account <- dayAccountStats.map(x=>x.accountId).distinct
        stat <- dayAccountStats if stat.transactionDay == day && stat.accountId == account
      } yield (account, day ) -> stat
    ).toMap

    val completeDayAccountStats = ( 
      for {
        day <- 1 to 31
        account <- dayAccountStats.map(x=>x.accountId).distinct
      } yield mapDayAccountStats.getOrElse((account,day),DayAccountStats(day,account,-1D,-1D,-1D,-1D,-1D))
    ).toList

    assert(dayAccountStats.map(x=>x.accountId).distinct.size * 31 == completeDayAccountStats.size)
  }
}
