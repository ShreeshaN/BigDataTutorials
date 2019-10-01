package com.project1.hadoop.queries.dataset;

import com.project1.hadoop.beans.Customer;
import com.project1.hadoop.beans.Transaction;
import com.utils.GeneralUtilities;

import java.util.ArrayList;
import java.util.List;


public class DataGenerator {
    /**
     * This file is used to generate customer and transactions data according to specification.
     *
     * @param args default arguments
     */

    public static void main(String[] args) {
        int customerSize = 30;
        int transactionsPerCustomer = 5;
        CustomerDataset customerGen = new CustomerDataset();
        TransactionDataset transactionGen = new TransactionDataset();
        List<Customer> customers = new ArrayList<>();
        List<Transaction> transactions = new ArrayList<>();
        int transactionId = 1;
        for (int customerId = 1; customerId <= customerSize; customerId++) {
            Customer customer = customerGen.generateCustomerForCustomerId(customerId);
            int totalTransCount = transactionsPerCustomer * customerId; // Doing this logic as we need a global transaction ID
            while (transactionId <= totalTransCount) {
                Transaction transaction = transactionGen.CreateTransactionForTransactionIdAndCustomerId(transactionId, customerId);
                transactions.add(transaction);
                transactionId++;
            }
            customers.add(customer);
        }
        GeneralUtilities.writeIterableToFile(customers, "/Users/badgod/badgod_documents/customers.txt");
        GeneralUtilities.writeIterableToFile(transactions, "/Users/badgod/badgod_documents/transactions.txt");
    }
}
