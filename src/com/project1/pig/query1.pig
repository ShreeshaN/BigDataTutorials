/**
* Write an Apache Pig query that reports the customer names that have the least number of transactions.
* Your output should be the customer names, and the number of transactions
*/

customers = LOAD '/Users/badgod/badgod_documents/customers.txt' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);
limit_data = LIMIT customers 2;
DUMP limit_data;

transactions = LOAD '/Users/badgod/badgod_documents/transactions.txt' USING PigStorage(',') AS
    (transactionId:int, transCustomerId:int, transactionTotal:float, transactionNumItems:int, transactionDesc:chararray);
limit_data = LIMIT transactions 10;
DUMP limit_data;

joined = JOIN customers by customerId, transactions by transCustomerId;
limit_data = LIMIT joined 5;
DUMP limit_data;

groupedCustomers = GROUP joined by customerName;
limit_data = LIMIT groupedCusotmers 10;
DUMP limit_data;

result = FOREACH groupedCustomers GENERATE group, COUNT(joined);
DUMP result;
