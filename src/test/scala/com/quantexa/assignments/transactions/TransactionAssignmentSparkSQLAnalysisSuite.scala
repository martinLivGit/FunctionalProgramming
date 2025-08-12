package com.quantexa.assignments.transactions

import org.scalatest.FunSuite

class TransactionAssignmentSparkSQLAnalysisSuite extends TransactionAssignmentBaseSuite {

   val accountStats: List[DayAccountStats] = TransactionAssignmentSparkSQL(transactions)

   def withFixture(test: OneArgTest) = {

      // create the fixture
      val theFixture = FixtureParam(accountStats)

      try {
         withFixture(test.toNoArgTest(theFixture)) // "loan" the fixture to the test
      }
   }

}
