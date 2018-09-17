package com.project.spark.objects;

import java.io.Serializable;

public class CustomerSales implements Serializable {
    private static final long serialVersionUID = -6375906856056172937L;
    private String state;
    private String year;
    private String month;
    private String day;
    private String hour;
    private Long sales;


    public CustomerSales(String state, String year, String month, String day, String hour,
            Long sales) {
        this.state = state;
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.sales = sales;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getHour() {
        return hour;
    }

    public void setHour(String hour) {
        this.hour = hour;
    }

    public Long getSales() {
        return sales;
    }

    public void setSales(Long sales) {
        this.sales = sales;
    }

}
