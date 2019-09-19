/**
* Write an Apache Pig query that reports the Country Codes having number of customers greater than 5,000 or less than 2,000.
*/

customers = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/kingspp_data/customers_sample.csv' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);
limit_data = LIMIT customers 2;
DUMP limit_data;


groupedCustomers = GROUP customers by countryCode;
limit_data = LIMIT groupedCustomers 10;
DUMP limit_data;

result = FOREACH groupedCustomers GENERATE group, COUNT(customers.customerName) as customerCount;

filteredResult = FILTER result by customerCount<2000 or customerCount>5000;
dump filteredResult;
