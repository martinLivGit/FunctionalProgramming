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

object AddressData {
  implicit def orderingByIdFromDate: Ordering[AddressData] =
    Ordering.by(a => (a.addressId, a.fromDate))
}

object GroupOccupancy {

  //logic check for shared/overlapping occupancy
  private def overlappingOccupant(addr: AddressData, grp: GroupData): Boolean = addr.addressId == grp.addressId && (grp.fromDate to grp.toDate).contains(addr.fromDate)
   
  private def aggOp( groupedOccupants:List[GroupData],occ:AddressData):List[GroupData] =
  {
    groupedOccupants match {
      case grp :: grpTail if overlappingOccupant(occ,grp) => //Add current occupant to the current group and update the group toDate if current occupant is greater
        val grpToDate = if (grp.toDate > occ.toDate) grp.toDate else occ.toDate
        grp.copy(customerIds=grp.customerIds :+ occ.customerId,toDate=grpToDate) :: grpTail
      case grp :: _ => //Create a new occupancy group and add to the list of occupancy groups
        GroupData(grp.groupId+1,Seq(occ.customerId),occ.addressId,occ.fromDate,occ.toDate) :: groupedOccupants
      case Nil => //Create an initial occupancy group and add to the list of occupancy groups
        GroupData(1,Seq(occ.customerId),occ.addressId,occ.fromDate,occ.toDate) :: Nil
    }
  }

  //takes a start and fold occupants return updated accumulator
  def apply(occupants : List[AddressData]): List[GroupData] = {
    occupants.sorted.foldLeft(List.empty[GroupData])(aggOp)
  }

}
