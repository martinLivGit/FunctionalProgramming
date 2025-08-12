package com.quantexa.assignments.addresses

import org.scalatest.FunSuite

class AddressAssignmentSolutionSuite extends FunSuite {

   /*    IND0003	      ADR001	    727	        803
   *     IND0004	      ADR003	    651	        820
   *     IND0007	      ADR003	    1710	      1825
   *     IND0008	      ADR005	    29	        191
   *     IND0001	      ADR003	    1777	      1825
   *     IND0002	      ADR003	    1144        1158*/

   val addressDataList : List[AddressData] = List[AddressData](
                        AddressData("MARTIN","STIRLING",651,820 )
                        ,AddressData("BOB","STIRLING",819,820 )
                        ,AddressData("JACK","STIRLING",821,823 )
                        ,AddressData("BILL","PERTH",819,820 )
                     )

   val groupedOccupancy: List[GroupData] = GroupOccupancy(addressDataList)

   test("addressDataList is not empty)") {
      assert(addressDataList.nonEmpty)
   }

   test("addressDataList size is 4") {
      assert(addressDataList.size == 4)
   }

   test("groupedOccupancy size is 3") {
      assert(groupedOccupancy.size == 3)
   }

   test("groupedOccupancy number of members is 4") {
      groupedOccupancy.foreach(println)
      println(groupedOccupancy.size)
      println(addressDataList.size)
      assert((groupedOccupancy map (p => p.customerIds.size)).sum == 4)
   }

   test("groupedOccupancy group id 1 ") {
      assert(groupedOccupancy(2).groupId == 1)
      assert(groupedOccupancy(2).addressId == "PERTH")
      assert(groupedOccupancy(2).fromDate == 819)
      assert(groupedOccupancy(2).toDate == 820)
      assert(groupedOccupancy(2).customerIds.size == 1)
      assert(groupedOccupancy(2).customerIds.head == "BILL")
   }

   test("groupedOccupancy group id 2 ") {
      assert(groupedOccupancy(1).groupId == 2)
      assert(groupedOccupancy(1).addressId == "STIRLING")
      assert(groupedOccupancy(1).fromDate == 651)
      assert(groupedOccupancy(1).toDate == 820)
      assert(groupedOccupancy(1).customerIds.size == 2)
      assert(groupedOccupancy(1).customerIds.head == "BOB")
      assert(groupedOccupancy(1).customerIds(1) == "MARTIN")
   }

   test("groupedOccupancy group id 3 ") {
      assert(groupedOccupancy.head.groupId == 3)
      assert(groupedOccupancy.head.addressId == "STIRLING")
      assert(groupedOccupancy.head.fromDate == 821)
      assert(groupedOccupancy.head.toDate == 823)
      assert(groupedOccupancy.head.customerIds.size == 1)
      assert(groupedOccupancy.head.customerIds.head == "JACK")
   }
}
