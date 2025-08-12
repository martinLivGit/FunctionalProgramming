package com.quantexa.assignments.transactions

//import org.scalatest.FunSuite
import org.scalatest.{Outcome, fixture}

abstract class TransactionAssignmentBaseSuite extends fixture.FunSuite {

   val transactions : List[Transaction] = List[Transaction](
                        Transaction("T00001","ACCOUNT1", 1, "AA", 200.00)
                        ,Transaction("T00002","ACCOUNT1", 2, "AA", 200.00)
                        ,Transaction("T00003","ACCOUNT1", 3, "AA", 200.00)
                        ,Transaction("T00004","ACCOUNT1", 4, "AA", 200.00)
                        ,Transaction("T00005","ACCOUNT1", 5, "AA", 200.00)
                        ,Transaction("T00006","ACCOUNT1", 5, "CC", 200.00)
                        ,Transaction("T00007","ACCOUNT2", 1, "AA", 100.00)
                        ,Transaction("T00008","ACCOUNT2", 2, "AA", 100.00)
                        ,Transaction("T00009","ACCOUNT2", 3, "AA", 100.00)
                        ,Transaction("T00010","ACCOUNT2", 4, "AA", 100.00)
                        ,Transaction("T00011","ACCOUNT2", 5, "AA", 100.00)
                        ,Transaction("T00012","ACCOUNT2", 5, "CC", 100.00)
                        )

   case class FixtureParam(accountStats: List[DayAccountStats])

   def withFixture(test: OneArgTest) : Outcome;

   //val accountStats: List[DayAccountStats]

   //DayAccountStats(2,ACCOUNT1,200.0,200.0,200.0,0.0,0.0)
   test("dayAccountStats day 2 and ACCOUNT1") { f =>
      assert( f.accountStats
                  .filter(p => p.transactionDay == 2 & p.accountId == "ACCOUNT1")
                  .head
            == DayAccountStats(2,"ACCOUNT1",200,200,200,0,0))
   }

   test("dayAccountStats day 2 and ACCOUNT2") { f =>
      assert( f.accountStats
                  .filter(p => p.transactionDay == 2 & p.accountId == "ACCOUNT2")
                  .head
            == DayAccountStats(2,"ACCOUNT2",100,100,100,0,0))
   }

   //DayAccountStats(5,ACCOUNT1,200.0,200.0,800.0,0.0,0.0)
   test("dayAccountStats day 5 and ACCOUNT1") { f =>
      assert( f.accountStats
                  .filter(p => p.transactionDay == 5 & p.accountId == "ACCOUNT1")
                  .head
            == DayAccountStats(5,"ACCOUNT1",200,200,800,0,0))
   }

   //DayAccountStats(5,ACCOUNT2,100.0,100.0,400.0,0.0,0.0)
   test("dayAccountStats day 5 and ACCOUNT2") { f =>
      assert( f.accountStats
                  .filter(p => p.transactionDay == 5 & p.accountId == "ACCOUNT2")
                  .head
            == DayAccountStats(5,"ACCOUNT2",100,100,400,0,0))
   }

}
