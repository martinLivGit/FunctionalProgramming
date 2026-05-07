# FunctionalProgramming

## Spark/Scala Programming Assessment
- Addresses: written in a functional programming style avoiding the use of variables and loops etc.
- Transactions: implementing custom sliding window and aggregation respectively - avoiding sql functions. A couple of solutions are offered here:
  1. Implementation using filters/map on transaction lists.
  2. Implementation using fold on transaction lists.
- Accounts: written using the Datasets API. Joining datasets and performing aggregration whilst avoiding the use of SQL functions.

Basic tests are provided.

## Explanation

### Addresses

Good to understand recursive processing, use of accumulators and Scala tail recursion. You can choose to write your own tail recursive solution as I have done in the change history. Ultimately, you can recognise that the same thing can be achieved by folding the input list to produce an output list of groups of overlapping occupants. Note: Fold uses multiple parameter lists so its good to have familiarity of currying. Also, good to have familiarity with List pattern matching.

### Transactions

Good to understand flatMap, filter, map chaining and the syntactic-sugar of for comprehension - so can implement with either - best to start with flatMap, filter, map. Subsequently, need to get the grouping and mapping and also pass a valid aggregation function to map. I provided a couple of examples of functions - again folding will work. Good to understand nested tuples and associated pattern matching.

### Accounts

Good to understand: Spark, Datasets API, join types and the joins which preserve datatypes. Similar pattern as above, that is, grouping, mapping and passing an aggregation function.
