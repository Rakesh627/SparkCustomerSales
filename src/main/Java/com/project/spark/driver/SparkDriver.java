package com.project.spark.driver;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.project.spark.hdfs.saver.HDFSFileSaver;
import com.project.spark.objects.Customer;
import com.project.spark.objects.CustomerSales;
import com.project.spark.objects.Sale;
import com.project.spark.processor.CustomerProcessor;
import com.project.spark.processor.CustomerSalesProcessor;
import com.project.spark.processor.SaleProcessor;
import com.project.spark.properties.Properties;

/**
 * This is the spark driver which initiates the connection to Apache Spark
 * 
 * @author rsuresh
 *
 */
public class SparkDriver {
    private static SparkContext context = null;
    private CustomerProcessor customerProcessor = new CustomerProcessor();
    private SaleProcessor saleProcessor = new SaleProcessor();
    private CustomerSalesProcessor customerSalesProcessor = new CustomerSalesProcessor();
    private HDFSFileSaver fileSaver = new HDFSFileSaver();
    private Set<String> stateSet = new HashSet<>();
    
    // init during class initialization
    static {
        SparkConf conf =
                new SparkConf().setAppName(Properties.APP_NAME).setMaster(Properties.SPARK_MASTER_URL);
        context = new SparkContext(conf);
    }

    // make it accessible across all classes
    public static SparkContext getSparkContext() {
        return context;
    }

    /**
     * Creates RDD using the customer & sales files and joins them to perform group by operations
     * @param outputFileLocation 
     * @param salesFileLocation 
     * @param customerFileLocation 
     */
	private void processCustomerSalesInformation(String customerFileLocation, String salesFileLocation, String outputFileLocation) {
		// Step 1: process customer data from HDFS
		JavaPairRDD<Integer, Customer> customers = customerProcessor.processCustomerInformation(customerFileLocation,this.stateSet);

		// Step 2: process sales data from HDFS
		JavaPairRDD<Integer, Sale> sales = saleProcessor.processSalesInformation(salesFileLocation);

		// Step 3: join customer and sales using customer Id
		JavaRDD<CustomerSales> customerAndSaleJoin = customerSalesProcessor
				.combineCustomerAndSalesInformation(customers, sales);

		//Step 4: perform group by operations and get result with key=state, value=>group by result
		JavaPairRDD<String, String> byState = customerSalesProcessor.groupByState(customerAndSaleJoin);
		JavaPairRDD<String, String> byStateYear = customerSalesProcessor.groupByStateYear(customerAndSaleJoin);
		JavaPairRDD<String, String> byStateYearMonth = customerSalesProcessor
				.groupByStateYearMonth(customerAndSaleJoin);
		JavaPairRDD<String, String> byStateYearMonthDay = customerSalesProcessor
				.groupByStateYearMonthDay(customerAndSaleJoin);
		JavaPairRDD<String, String> byStateYearMonthDayHour = customerSalesProcessor
				.groupByStateYearMonthDayHour(customerAndSaleJoin);

		// Step 6: we union the grouped data
		JavaPairRDD<String, String> unSortedGroupedRDD = byState.union(byStateYear).union(byStateYearMonth)
				.union(byStateYearMonthDay).union(byStateYearMonthDayHour);

		// Step 7: we sort the data by states
		JavaPairRDD<String, String> finalGroupByRDD = unSortedGroupedRDD.sortByKey();
		
		// Step 8: store it to the file system
		fileSaver.saveToFS(finalGroupByRDD,outputFileLocation);
	}
	
	// entry point
    public static void main(String[] args) {
        SparkDriver driver = new SparkDriver();
        String customerFileLocation = "";
        String salesFileLocation = "";
        String outputFileLocation = "";

        if(args.length>0) {
        		for(int i=0;i<args.length;i++) {
        			
        			//help
        			if(args[i].equalsIgnoreCase("-h") || args[i].equalsIgnoreCase("-help")) {
        				System.out.println("-c is for Customer file location, can be hdfs or local file system");
        				System.out.println("-s is for Sales file location, can be hdfs or local file system");
        				System.out.println("-o is for output file location, can be hdfs or local file system");
        				System.out.println("-state is a state filter, provide a list of states delimited by ','");
        				System.exit(0);
        			}

        			//-c is for Customer file location, can be hdfs or local file system
        			if(args[i].equalsIgnoreCase("-c")) {
        				if(args.length!=i+1) {
        					customerFileLocation = args[i+1];
        					i++;
        				}
        			}
        			
        			//-s is for Sales file location, can be hdfs or local file system
        			else if(args[i].equalsIgnoreCase("-s")) {
        				if(args.length!=i+1) {
        					salesFileLocation = args[i+1];
        					i++;
        				}
        			}
        			
        			//-o is for output file location, can be hdfs or local file system
        			else if(args[i].equalsIgnoreCase("-o")) {
        				if(args.length!=i+1) {
        					outputFileLocation = args[i+1];
        					i++;
        				}
        			}
        			
        			//-state is a state filter, provide a list of states delimited by ','
        			else if(args[i].equalsIgnoreCase("-state")) {
        				if(args.length!=i+1) {
        					String states = args[i+1];
						driver.stateSet = new HashSet<>(Arrays.asList(states.split(",")).stream()
								.map(String::toLowerCase).collect(Collectors.toList()));
        					i++;
        				}
        			}
        		}
        }
        
        driver.processCustomerSalesInformation(customerFileLocation,salesFileLocation,outputFileLocation);
    }

}
