/**
 * @author rsuresh
 *
 */
package com.project.spark.properties;

public class Properties {
    public final static String APP_NAME = "Spark-APP";
    public final static String SPARK_MASTER_URL = "local[2]";
    public final static String CUSTOMER_LINE_REGEX = "[\\d]+#[^#]+#[^#]+#[\\d]+";
    public final static String SALES_LINE_REGEX = "([\\d]{10}|[\\d]{13})#[\\d]+#[\\d]+";
    //LOCAL HADOOP
    public final static String CUSTOMER_HDFS_PATH = "hdfs://localhost:8020//user/hadoop/sparkjob/customer.txt";
    public final static String SALES_HDFS_PATH = "hdfs://localhost:8020//user/hadoop/sparkjob/sales.txt";
    public final static String OUTPUT_HDFS_PATH = "hdfs://localhost:8020//user/hadoop/sparkjob/output";
}
