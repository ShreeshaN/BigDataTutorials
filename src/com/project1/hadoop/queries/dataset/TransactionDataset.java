package com.project1.hadoop.queries.dataset;

import com.project1.hadoop.utils.GeneralUtilities;
import com.project1.hadoop.utils.StringConstants;
import com.project1.hadoop.beans.Transaction;

import java.util.Random;

public class TransactionDataset {

    /**
     * TransID: unique sequential number (integer) from 1 to 5,000,000 (the file has 5M transactions)
     * CustID: References one of the customer IDs, i.e., from 1 to 50,000 (on Avg. a customer has 100 trans.)
     * TransTotal: random number (float) between 10 and 1000
     * TransNumItems: random number (integer) between 1 and 10
     * TransDesc: random text of characters of length between 20 and 50 (do not include commas)
     */


    private Random random = new Random();

    private float getTransactionTotal() {
        int low = 10;
        int high = 1000;
        return (float) GeneralUtilities.getRandomNumberBetweenRange(low, high);
    }

    private int getTransactionNumItems() {
        int low = 1;
        int high = 10;
        return GeneralUtilities.getRandomNumberBetweenRange(low, high);
    }

    private String getTransactionDescription() {
        String alphabets = StringConstants.ALPHABETS;
        int alphabetLen = alphabets.length();
        StringBuilder desc = new StringBuilder();
        int low = 20;
        int high = 50;
        int nameLength = GeneralUtilities.getRandomNumberBetweenRange(low, high);
        for (int i = 0; i < nameLength; i++) {
            desc.append(alphabets.charAt(random.nextInt(alphabetLen)));
        }
        return desc.toString();

    }

    public Transaction CreateTransactionForTransactionIdAndCustomerId(int transactionId, int customerId) {
        float transactionTotal = getTransactionTotal();
        Integer transactionNumItems = getTransactionNumItems();
        String transactionDescription = getTransactionDescription();
        return new Transaction(transactionId, customerId, transactionTotal, transactionNumItems, transactionDescription);
    }
}
