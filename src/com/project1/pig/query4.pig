/**
* Assume we want to design an analytics task on the data as follows:
  1) The Age attribute is divided into six groups, which are [10, 20), [20, 30), [30, 40), [40, 50), [50, 60), and [60, 70].
     The bracket “[“ means the lower bound of a range is included, where as “)” means the upper bound of a range is excluded.
  2) Within each of the above age ranges, further division is performed based on the “Gender”, i.e.,
     each of the 6 age groups is further divided into two groups.
  3) For each group, we need to report the following info:
  --> Age Range, Gender, MinTransTotal, MaxTransTotal, AvgTransTotal
*/


customers = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/customers.txt' USING PigStorage(',') AS
    (customerId:int, customerName:chararray, age:int, gender:chararray, countryCode:int, salary:float);

transactions = LOAD '/Users/badgod/badgod_documents/github/BigDataTutorials/input/shree_data/transactions.txt' USING PigStorage(',') AS
    (transactionId:int, transCustomerId:int, transactionTotal:float, transactionNumItems:int, transactionDesc:chararray);

joined = JOIN customers by customerId, transactions by transCustomerId;

AgeLessThan10 = FILTER joined BY (chararray)SIZE((chararray)age) == '1';
AgeMoreThan10 = FILTER joined BY (chararray)SIZE((chararray)age) != '1';

ageRangeForAgeLessThan10 = FOREACH AgeLessThan10 GENERATE customerId as cid, '0-10' as ageRange;

ageRangeForAgeMoreThan10 = FOREACH AgeMoreThan10
{
    firstStr = CONCAT(SUBSTRING((chararray)age,0,1),'0');
    secondStr = CONCAT((chararray)((int)SUBSTRING((chararray)age,0,1)+1),'0');
    midStr='-';
    finalStr = CONCAT(firstStr,midStr,secondStr);
    ageRange = finalStr;
    GENERATE customerId as cid, ageRange;
}

customerIdWithAgeRanges = UNION ageRangeForAgeLessThan10, ageRangeForAgeMoreThan10;

grouped = GROUP customerIdWithAgeRanges by cid;

distinctGrouped = FOREACH grouped {
b = customerIdWithAgeRanges.(ageRange);
c = distinct b;
generate group as cid, flatten(c) as ageRange;
}

joinedCustomers = JOIN distinctGrouped by cid, customers by customerId;

joinedCustomersAndTransactions = JOIN joinedCustomers by cid, transactions by transCustomerId;

groupedCustomers = GROUP joinedCustomersAndTransactions by (ageRange, gender);

result = FOREACH groupedCustomers GENERATE group,
                MIN(joinedCustomersAndTransactions.transactionTotal),
                MAX(joinedCustomersAndTransactions.transactionTotal),
                AVG(joinedCustomersAndTransactions.transactionTotal);
dump result;