/**
* Write an Apache Pig query that reports the customer names that have the least number of transactions.
* Your output should be the customer names, and the number of transactions
*/

customers = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt' USING PigStorage(',') AS
    (transactionId:int, transCustomerId:int, transactionTotal:float, transactionNumItems:int, transactionDesc:chararray);

joined = JOIN customers by customerId, transactions by transCustomerId;

groupedCustomers = GROUP joined by customerId;

customersWithTransactionCount = FOREACH groupedCustomers GENERATE group as customerName, COUNT(joined) as transactionCount;

minVal = LIMIT (ORDER (foreach customersWithTransactionCount generate transactionCount) by transactionCount ASC) 1;

res = filter customersWithTransactionCount by transactionCount==minVal.$0;
dump res;