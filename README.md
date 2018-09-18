Project Information
-------------------


The project takes in a customer & sales data and transforms them into customer-sales data for further processing. 
It currently performs groupBy operations in the following criteria:

1. Group by State
2. Group by State and Year
3. Group by State, Year and Month
4. Group by State, Year, Month and Day
5. Group by State, Year, Month, Day and Hour.

To view the results for a particular state please use the arguement `-state <List Of States by ','>`.

Installation
------------
Clone the repository and import the project into your IDE and update the Maven project to get all the jar files required for the project. Importing into a IDE like eclipse automatically does this once imported.

The project `default configuration` is on the Properites.java file with default location. Please modify this to your setting or use the command line parameters listed below.

Build a jar file out of the project and you can submit the jar to your spark cluster. 

To Build project:

`cd ~/SparkCustomerSales`

`mvn clean install`

Following updating the maven resources, you can go ahead and import.

Input Arguements
----------------

`-c is for Customer file location, can be hdfs or local file system`

`-s is for Sales file location, can be hdfs or local file system`

`-o is for output file location, can be hdfs or local file system`

`-state is a state filter, provide a list of states delimited by ','`

The hdfs or local file system URL's can be changed below appropriately, depending on the requirement.

Running the project in Yarn
----------------------------

`spark-submit --master yarn `

`--class com.project.spark.driver.SparkDriver /home/hadoop/spark.jar \`

`-c hdfs://localhost:8020/user/hadoop/sparkjob/test-customer.txt \`

`-s hdfs://localhost:8020/user/hadoop/sparkjob/test-sales.txt \`

`-o hdfs://localhost:8020/user/hadoop/sparkjob/output \`

`-state AL,AK`

Running the project in standalone or local
------------------------------------------

`spark-submit --master local `

`--class com.project.spark.driver.SparkDriver /home/hadoop/spark.jar \`

`-c hdfs://localhost:8020/user/hadoop/sparkjob/test-customer.txt \`

`-s hdfs://localhost:8020/user/hadoop/sparkjob/test-sales.txt \`

`-o hdfs://localhost:8020/user/hadoop/sparkjob/output \`

`-state AL,AK`

Running the project with large dataset
---------------------------------------

`spark-submit --master local `

`--executor-memory 4G \`

`--driver-memory 4G \`

`--class com.project.spark.driver.SparkDriver /home/hadoop/spark.jar \`

`-c hdfs://localhost:8020/user/hadoop/sparkjob/test-customer.txt \`

`-s hdfs://localhost:8020/user/hadoop/sparkjob/test-sales.txt \`

`-o hdfs://localhost:8020/user/hadoop/sparkjob/output \`

`-state AL,AK`



