/**
*
* Write an Apache Pig query that join Customers and Transactions using Broadcast (replicated) join.
* The query reports for each customer the following info:
* --> CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
* Where NumOfTransactions is the total number of transactions done by the customer,
* TotalSum is the sum of field “TransTotal” for that customer,
* and MinItems is the minimum number of items in transactions done by the customer.
*/


customers = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/kingspp_data/customers_sample.csv' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);
limit_data = LIMIT customers 2;
DUMP limit_data;

transactions = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/kingspp_data/transactions_sample.csv' USING PigStorage(',') AS
    (transactionId:int, transCustomerId:int, transactionTotal:float, transactionNumItems:int, transactionDesc:chararray);
limit_data = LIMIT transactions 10;
DUMP limit_data;

joined = JOIN customers by customerId, transactions by transCustomerId USING 'replicated';
limit_data = LIMIT joined 10;
DUMP limit_data;

groupedCustomers = GROUP joined by customerId;
limit_data = LIMIT groupedCustomers 10;
DUMP limit_data;

result = FOREACH groupedCustomers GENERATE group,
                joined.customerName,
                joined.salary,
                SUM(joined.transactionNumItems),
                SUM(joined.transactionTotal),
                MIN(joined.transactionNumItems);

result = FOREACH groupedCustomers {
    b = joined.(customerName, salary);
    distinctCustomerNameAndSalary = distinct b;
    generate
         group,
         FLATTEN(distinctCustomerNameAndSalary),
         SUM(joined.transactionNumItems),
         SUM(joined.transactionTotal),
         MIN(joined.transactionNumItems);
}
DUMP result;

