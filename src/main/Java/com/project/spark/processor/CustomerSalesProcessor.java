package com.project.spark.processor;

import java.util.Calendar;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import com.project.spark.objects.Customer;
import com.project.spark.objects.CustomerSales;
import com.project.spark.objects.Sale;

import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;
import scala.Tuple5;
/**
 * Process customer sales data
 * @author rsuresh
 *
 */
public class CustomerSalesProcessor {
	
    /**
     * Combines customer and sales PairRDD using customerId key and return a combined CustomerSales
     * POJO. CustomerSales can be used to manipulate the data further.
     * 
     * @param customers of type JavaPairRDD<Integer, Customer>
     * @param sales of type JavaPairRDD<Integer, Sale>
     * @return JavaRDD<CustomerSales> combined data from both RDD's
     * ex:
     *  { 
     *      "state": AL,
     *      "year": 2018,
     *      "month": 08,
     *      "day": 12,
     *      "hour": 07,
     *      "sales": 6789 
     *  }
     */
    public JavaRDD<CustomerSales> combineCustomerAndSalesInformation(
            JavaPairRDD<Integer, Customer> customers, JavaPairRDD<Integer, Sale> sales) {
        // join the pair RDD's and store it in POJO CustomerSales
        JavaRDD<CustomerSales> customerAndSaleJoin = customers.join(sales).map(tuple -> {
            Customer customer = tuple._2._1;
            Sale sale = tuple._2._2;
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(sale.getEpoch() * 1000L);

            // %02d appends 0 with any single digit numbers
            CustomerSales value =
                    new CustomerSales(customer.getState(), String.valueOf(cal.get(Calendar.YEAR)),
                            // Calendar.MONTH is from 0 to 11, so we add 1
                            String.format("%02d", cal.get(Calendar.MONTH) + 1),
                            String.format("%02d", cal.get(Calendar.DATE)),
                            String.format("%02d", cal.get(Calendar.HOUR)), sale.getSale());
            return value;
        });
        return customerAndSaleJoin;
    }

	/**
	 * Does group by operation on the data using state
	 * 
	 * @param customerAndSaleJoin
	 *            of type JavaRDD<CustomerSales>
	 * @return
	 */
	public JavaPairRDD<String, String> groupByState(JavaRDD<CustomerSales> customerAndSaleJoin) {
		// group by state ex: AL#####123457
		JavaPairRDD<String, String> combineByState = customerAndSaleJoin
				.mapToPair(customerSales -> new Tuple2<>(customerSales.getState(), customerSales.getSales()))
				.reduceByKey((sale1, sale2) -> sale1 + sale2).mapToPair(data -> {
					return new Tuple2<>(data._1, data._1 + "#####" + data._2);
				});
		return combineByState;

	}

	/**
	 * Does group by operation on the data using state and year
	 * 
	 * @param customerAndSaleJoin
	 *            of type JavaRDD<CustomerSales>
	 * @return
	 */
	public JavaPairRDD<String, String> groupByStateYear(JavaRDD<CustomerSales> customerAndSaleJoin) {
		// group by state and year, ex: AL#2017####123457
		JavaPairRDD<String, String> combineByStateYear = customerAndSaleJoin
				.mapToPair(customerSales -> new Tuple2<>(
						new Tuple2<>(customerSales.getState(), customerSales.getYear()), customerSales.getSales()))
				.reduceByKey((sale1, sale2) -> sale1 + sale2).mapToPair(data -> {
					return new Tuple2<>(data._1._1, data._1._1 + "#" + data._1._2 + "####" + data._2);
				});
		return combineByStateYear;
	}

	/**
	 * Does group by operation on the data using state, year and month
	 * 
	 * @param customerAndSaleJoin
	 *            of type JavaRDD<CustomerSales>
	 * @return
	 */
	public JavaPairRDD<String, String> groupByStateYearMonth(JavaRDD<CustomerSales> customerAndSaleJoin) {
		// group by state, year and month, ex: AL#2017#08###123457
		JavaPairRDD<String, String> combineByStateYearMonth = customerAndSaleJoin
				.mapToPair(customerSales -> new Tuple2<>(
						new Tuple3<>(customerSales.getState(), customerSales.getYear(), customerSales.getMonth()),
						customerSales.getSales()))
				.reduceByKey((sale1, sale2) -> sale1 + sale2).mapToPair(data -> {
					return new Tuple2<>(data._1._1(),
							data._1._1() + "#" + data._1._2() + "#" + data._1._3() + "###" + data._2);
				});
		return combineByStateYearMonth;

	}

	/**
	 * Does group by operation on the data using state, year, month and day
	 * 
	 * @param customerAndSaleJoin
	 *            of type JavaRDD<CustomerSales>
	 * @return
	 */
	public JavaPairRDD<String, String> groupByStateYearMonthDay(JavaRDD<CustomerSales> customerAndSaleJoin) {
		// group by state, year, month and day, ex: AL#2017#08#01##123457
		JavaPairRDD<String, String> combineByStateYearMonthDay = customerAndSaleJoin
				.mapToPair(customerSales -> new Tuple2<>(new Tuple4<>(customerSales.getState(), customerSales.getYear(),
						customerSales.getMonth(), customerSales.getDay()), customerSales.getSales()))
				.reduceByKey((sale1, sale2) -> sale1 + sale2).mapToPair(data -> {
					return new Tuple2<>(data._1._1(), data._1._1() + "#" + data._1._2() + "#" + data._1._3() + "#"
							+ data._1._4() + "##" + data._2);
				});
		return combineByStateYearMonthDay;
	}

	/**
	 * Does group by operation on the data using state, year, month, day and hour
	 * 
	 * @param customerAndSaleJoin
	 *            of type JavaRDD<CustomerSales>
	 * @return
	 */
	public JavaPairRDD<String, String> groupByStateYearMonthDayHour(JavaRDD<CustomerSales> customerAndSaleJoin) {
		// group by state, year, month, day and hour, ex: AL#2017#08#01#09#123457
		JavaPairRDD<String, String> combineByStateYearMonthDayHour = customerAndSaleJoin
				.mapToPair(
						customerSales -> new Tuple2<>(
								new Tuple5<>(customerSales.getState(), customerSales.getYear(),
										customerSales.getMonth(), customerSales.getDay(), customerSales.getHour()),
								customerSales.getSales()))
				.reduceByKey((sale1, sale2) -> sale1 + sale2).mapToPair(data -> {
					return new Tuple2<>(data._1._1(), data._1._1() + "#" + data._1._2() + "#" + data._1._3() + "#"
							+ data._1._4() + "#" + data._1._5() + "#" + data._2);
				});
		return combineByStateYearMonthDayHour;
	}

}
