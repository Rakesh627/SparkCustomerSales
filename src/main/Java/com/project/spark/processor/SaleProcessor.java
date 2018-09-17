package com.project.spark.processor;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.project.spark.driver.SparkDriver;
import com.project.spark.objects.Sale;
import com.project.spark.properties.Properties;

import scala.Tuple2;
/**
 * Process sales data
 * @author rsuresh
 *
 */
public class SaleProcessor {
    SparkContext context = SparkDriver.getSparkContext();

    /**
     * Gets sales data from files, loads into RDD and applies the transformation
     * @param salesFileLocation 
     * 
     * @return a RDD with customer Id and sales POJO 
     * { 
     *      "sale": 56789, 
     *      "epoch": 147288299001,
     *      "customerId": 1234 
     * }
     */
    public JavaPairRDD<Integer, Sale> processSalesInformation(String salesFileLocation) {
		if(salesFileLocation.isEmpty()) {
			salesFileLocation = Properties.SALES_HDFS_PATH;
		}

        JavaRDD<String> salesInfos = context.textFile(salesFileLocation,16).toJavaRDD();

        // convert Sales data into POJO => Sale and store in RDD using customer-id key
        JavaPairRDD<Integer, Sale> sales = salesInfos
                .filter(line -> line.matches(Properties.SALES_LINE_REGEX) == true).map(line -> {
                    String[] values = line.split("#");
                    Long epoch = Long.parseLong(values[0]);
                    Integer customerId = Integer.parseInt(values[1]);
                    Long sale = Long.parseLong(values[2]);
                    return new Sale(sale, epoch, customerId);
                }).mapToPair(sale -> new Tuple2<>(sale.getCustomerId(), sale));
        return sales;
    }

}
