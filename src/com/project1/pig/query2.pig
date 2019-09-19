/**
*
* Write an Apache Pig query that join Customers and Transactions using Broadcast (replicated) join.
* The query reports for each customer the following info:
* --> CustomerID, Name, Salary, NumOf Transactions, TotalSum, MinItems
* Where NumOfTransactions is the total number of transactions done by the customer,
* TotalSum is the sum of field “TransTotal” for that customer,
* and MinItems is the minimum number of items in transactions done by the customer.
*/


customers = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt' USING PigStorage(',') AS
    (transactionId:int, transCustomerId:int, transactionTotal:float, transactionNumItems:int, transactionDesc:chararray);

joined = JOIN customers by customerId, transactions by transCustomerId USING 'replicated';

groupedCustomers = GROUP joined by customerId;

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

