package com.quantexa.assignments.transactions

//import org.scalatest.FunSuite
import org.scalatest.fixture

class TransactionAssignmentSolutionSuite extends TransactionAssignmentBaseSuite {

   val accountStats: List[DayAccountStats] = TransactionAssignment(transactions)

   def withFixture(test: OneArgTest) = {

      // create the fixture
      val theFixture = FixtureParam(accountStats)

      try {
         withFixture(test.toNoArgTest(theFixture)) // "loan" the fixture to the test
      }
   }

}
