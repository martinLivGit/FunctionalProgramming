package com.quantexa.assignments.addresses

import scala.annotation.tailrec

/***
  *  You have been given a dataset containing a list of addresses, along with customers who lived at the addresses
  *  and the start and end date that they lived there. Here is an example:
  *
  *     Customer ID 	Address ID 	From date 	To_date
  *     IND0003	      ADR001	    727	        803
  *     IND0004	      ADR003	    651	        820
  *     IND0007	      ADR003	    1710	      1825
  *     IND0008	      ADR005	    29	        191
  *     IND0001	      ADR003	    1777	      1825
  *     IND0002	      ADR003	    1144        1158
  *
  *  Write an algorithm for the following:
  *
  *  "For each address, calculate all of the groups of customers who have lived at the address at overlapping times."
  *
  *  Note that each customer in the group only needs to overlap with at least one other customer in the group, so there
  *  may be pairs of customers in the group who never lived at the address at the same time.
  *
  *  The algorithm should output the following columns:
  *  •	 The address
  *  •	 The list of customers in the group
  *  •	 The first date that any customer in the group lived at the address
  *  •	 The last date that any customer in the group lived at the address
  *
  *  Example single row of output:
  *
  *  Address_ID 	Group_Customers	    Group_Start	  Group_End
  *  ADR003	      [IND0001,IND0007]	  1710	        1825
  *
  */

//Define a case class AddressData which stores the occupancy data
case class AddressData(
  customerId: String
  ,addressId: String
  ,fromDate: Int
  ,toDate: Int
)

case class GroupData(
  groupId: Long
  ,customerIds: Seq[String]
  ,addressId: String
  ,fromDate: Int
  ,toDate: Int
)

object GroupOccupancy {

  //logic check for shared/overlapping occupancy
  private def sharedOccupancy(address: AddressData, group: GroupData): Boolean = {
    (address.addressId == group.addressId && address.fromDate >= group.fromDate
      && address.fromDate <= group.toDate)
  }

  private def compareAddressAndFromDate(address1: AddressData, address2: AddressData) : Boolean = {
    if (address1.addressId == address2.addressId) address1.fromDate < address2.fromDate
    else address1.addressId < address2.addressId
  }

  @tailrec
  private def groupOccupants(occupants: List[AddressData], groupedOccupants: List[GroupData]): List[GroupData] = {
    val newGroupedOccupants = (occupants, groupedOccupants) match {
      case (Nil, _) => //No further occupancyData so return the grouped occupancy data
        return groupedOccupants
      case (occ :: _, Nil) => //Create an initial occupancy group and add to the list of occupancy groups
        GroupData(1,Seq(occ.customerId),occ.addressId,occ.fromDate,occ.toDate) :: Nil
      case (occ :: _, grp :: grpTail) if sharedOccupancy(occ,grp) => //Process the head of the next occupant ie the head occupant
        val grpToDate = if (grp.toDate > occ.toDate) grp.toDate else occ.toDate
        grp.copy(customerIds=occ.customerId +: grp.customerIds,toDate=grpToDate) :: grpTail
      case (occ :: _, grp :: _) => //Create a new occupancy group and add to the list of occupancy groups
        GroupData(grp.groupId+1,Seq(occ.customerId),occ.addressId,occ.fromDate,occ.toDate) :: groupedOccupants    
    }
    // process the remaining occupancy records, ie the occupants tail, using tail recursion
    groupOccupants(occupants.tail, newGroupedOccupants)
  }

   //1.initialise the shared occupancy data to Nil and process all sorted occupants
   def apply(occupants : List[AddressData] ): List[GroupData] = {
      groupOccupants( occupants.sortWith(compareAddressAndFromDate), Nil)
   }
}
