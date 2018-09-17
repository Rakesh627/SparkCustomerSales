package com.project.spark.objects;

import java.io.Serializable;

public class Customer implements Serializable {
    private static final long serialVersionUID = 6016147218179049130L;
    private int customerId;
    private String addressLine1;
    private String addressLine2;
    private String state;
    private int zipCode;

    public Customer(int streetNumber, String addressLine1, String addressLine2, String state,
            int zipCode) {
        this.customerId = streetNumber;
        this.addressLine1 = addressLine1;
        this.addressLine2 = addressLine2;
        this.state = state;
        this.zipCode = zipCode;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setStreetNumber(int streetNumber) {
        this.customerId = streetNumber;
    }

    public String getAddressLine1() {
        return addressLine1;
    }

    public void setAddressLine1(String addressLine1) {
        this.addressLine1 = addressLine1;
    }

    public String getAddressLine2() {
        return addressLine2;
    }

    public void setAddressLine2(String addressLine2) {
        this.addressLine2 = addressLine2;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public int getZipCode() {
        return zipCode;
    }

    public void setZipCode(int zipCode) {
        this.zipCode = zipCode;
    }


}
