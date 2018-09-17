package com.project.spark.objects;

import java.io.Serializable;

public class Sale implements Serializable {
    private static final long serialVersionUID = -6470967085253456215L;
    private Long sale;
    private Long epoch;
    private Integer customerId;

    public Sale(Long sale, Long epoch, Integer customerId) {
        super();
        this.sale = sale;
        this.epoch = epoch;
        this.customerId = customerId;
    }

    public Long getSale() {
        return sale;
    }

    public void setSale(Long sales) {
        this.sale = sales;
    }

    public Long getEpoch() {
        return epoch;
    }

    public void setEpoch(Long epoch) {
        this.epoch = epoch;
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }
}
