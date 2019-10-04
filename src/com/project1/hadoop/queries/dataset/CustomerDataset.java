
/**
@created on: 18/3/19,
@author: Shreesha N,
@version: v0.0.1
@system name: badgod
Description:

..todo::

*/
package com.project1.hadoop.queries.dataset;

import com.project1.hadoop.beans.Customer;
import com.utils.GeneralUtilities;
import com.utils.StringConstants;

import java.util.Random;


public class CustomerDataset {

    /**
     * ID: unique sequential number (integer) from 1 to 50,000 (that is the file will have 50,000 line)
     * Name: random sequence of characters of length between 10 and 20 (do not include commas)
     * Age: random number (integer) between 10 to 70
     * Gender: string that is either “male” or “female”
     * CountryCode: random number (integer) between 1 and 10
     * Salary: random number (float) between 100 and 10000
     */


    private Random random = new Random();


    private String getRandomName() {
        String alphabets = StringConstants.ALPHABETS;
        int alphabetLen = alphabets.length();

        StringBuilder name = new StringBuilder();
        int low = 10;
        int high = 20;
        int nameLength = GeneralUtilities.getRandomNumberBetweenRange(low, high);
        for (int i = 0; i < nameLength; i++) {
            name.append(alphabets.charAt(random.nextInt(alphabetLen)));
        }
        return name.toString();
    }

    private String getRandomGender() {
        String[] genderPool = {"MALE", "FEMALE"};
        return genderPool[random.nextInt(2)];
    }

    private Integer getCountryCode() {
        int low = 1;
        int high = 10;
        return GeneralUtilities.getRandomNumberBetweenRange(low, high);
    }

    private Double getRandomSalary() {
        int low = 100;
        int high = 10000;
        return (double) GeneralUtilities.getRandomNumberBetweenRange(low, high);
    }

    private Integer getRandomAge() {
        int low = 10;
        int high = 70;
        return GeneralUtilities.getRandomNumberBetweenRange(low, high);
    }

    public Customer generateCustomerForCustomerId(int customerId) {
        CustomerDataset datagen = new CustomerDataset();
        String name = datagen.getRandomName();
        Integer age = datagen.getRandomAge();
        String gender = datagen.getRandomGender();
        Integer countryCode = datagen.getCountryCode();
        Double salary = datagen.getRandomSalary();
        return new Customer(customerId, name, gender, age, countryCode, salary);
    }


}