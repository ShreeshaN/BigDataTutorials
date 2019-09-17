package com.project1.hadoop.beans;

public class Transaction {

//    TransID: unique sequential number (integer) from 1 to 5,000,000 (the file has 5M transactions)
//    CustID: References one of the customer IDs, i.e., from 1 to 50,000 (on Avg. a customer has 100 trans.)
//    TransTotal: random number (float) between 10 and 1000
//    TransNumItems: random number (integer) between 1 and 10
//    TransDesc: random text of characters of length between 20 and 50 (do not include commas)

    private Integer transactionId;
    private Integer customerId;
    private float transactionTotal;
    private Integer transactionNumItems;
    private String transactionDescription;

    public Transaction(Integer transactionId, Integer customerId, float transactionTotal, Integer transactionNumItems, String transactionDescription) {
        this.transactionId = transactionId;
        this.customerId = customerId;
        this.transactionTotal = transactionTotal;
        this.transactionNumItems = transactionNumItems;
        this.transactionDescription = transactionDescription;
    }

    @Override
    public String toString() {
        return transactionId + "," + customerId + "," + transactionTotal + "," + transactionNumItems + "," + transactionDescription;
    }

    public Integer getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(Integer transactionId) {
        this.transactionId = transactionId;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public float getTransactionTotal() {
        return transactionTotal;
    }

    public void setTransactionTotal(float transactionTotal) {
        this.transactionTotal = transactionTotal;
    }

    public Integer getTransactionNumItems() {
        return transactionNumItems;
    }

    public void setTransactionNumItems(Integer transactionNumItems) {
        this.transactionNumItems = transactionNumItems;
    }

    public String getTransactionDescription() {
        return transactionDescription;
    }

    public void setTransactionDescription(String transactionDescription) {
        this.transactionDescription = transactionDescription;
    }
}
