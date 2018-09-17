package com.project.spark.processor;

import java.util.Set;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.project.spark.driver.SparkDriver;
import com.project.spark.objects.Customer;
import com.project.spark.properties.Properties;

import scala.Tuple2;
/**
 * Processes customer data
 * @author rsuresh
 *
 */
public class CustomerProcessor {
    SparkContext context = SparkDriver.getSparkContext();

    /**
     * Gets customer data from files, loads into RDD and applies the transformation
     * @param customerFileLocation 
     * 
     * @return a RDD with customer Id and customer POJO ex: 
     * { 
     *      "customerId": 123,
     *      "addressLine1": Street, 
     *      "addressLine2": City, 
     *      "state": CA, 
     *      "zipCode": 1234 
     * }
     */
    public JavaPairRDD<Integer, Customer> processCustomerInformation(String customerFileLocation, Set<String> stateSet) {
    		if(customerFileLocation.isEmpty()) {
    			customerFileLocation = Properties.CUSTOMER_HDFS_PATH;
    		}
        JavaRDD<String> customerInfos =
                context.textFile(customerFileLocation, 16).toJavaRDD();
        // convert customer data into POJO => Customer and store in RDD using customer-id key
		JavaPairRDD<Integer, Customer> customers = customerInfos
				.filter(line -> line.matches(Properties.CUSTOMER_LINE_REGEX) == true).map(line -> {
					String[] values = line.split("#");
					int streetNumber = Integer.parseInt(values[0]);
					String addressLine1 = values[1];
					String addressLine2 = values[2];
					String[] splitAddress = addressLine2.split(" ");
					String state = splitAddress[splitAddress.length - 1];
					int zipCode = Integer.parseInt(values[3]);
					return new Customer(streetNumber, addressLine1, addressLine2, state, zipCode);
				}).filter(customer -> stateSet.isEmpty() || stateSet.contains(customer.getState().toLowerCase()))
				.mapToPair(customer -> new Tuple2<>(customer.getCustomerId(), customer));

        return customers;
    }

}
