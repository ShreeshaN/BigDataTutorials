package com.project1.hadoop.beans;

public class Customer {

//    ID: unique sequential number (integer) from 1 to 50,000 (that is the file will have 50,000 line)
//    Name: random sequence of characters of length between 10 and 20 (do not include commas) Age: random number (integer) between 10 to 70
//    Gender: string that is either “male” or “female”
//    CountryCode: random number (integer) between 1 and 10
//    Salary: random number (float) between 100 and 10000

    private Integer ID;
    private String name;
    private String gender;
    private Integer age;
    private Integer countryCode;
    private Double salary;

    public Customer(Integer ID, String name, String gender, Integer age, Integer countryCode, Double salary) {
        this.ID = ID;
        this.name = name;
        this.gender = gender;
        this.age = age;
        this.countryCode = countryCode;
        this.salary = salary;
    }

    @Override
    public String toString() {
        return ID + "," + name + "," + age + "," + gender + "," + countryCode + "," + salary;
    }

    public void setID(Integer ID) {
        this.ID = ID;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public void setCountryCode(Integer countryCode) {
        this.countryCode = countryCode;
    }

    public void setSalary(Double salary) {
        this.salary = salary;
    }


    public Integer getID() {
        return ID;
    }

    public String getName() {
        return name;
    }

    public String getGender() {
        return gender;
    }

    public Integer getCountryCode() {
        return countryCode;
    }

    public Double getSalary() {
        return salary;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }
}

