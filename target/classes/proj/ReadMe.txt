The project takes in a customer & sales data and transforms them into customer-sales data for further processing:

Commands to pass the file:

-c is for Customer file location, can be hdfs or local file system
-s is for Sales file location, can be hdfs or local file system
-o is for output file location, can be hdfs or local file system
-state is a state filter, provide a list of states delimited by ','

Running the project:
spark-submit --master yarn --executor-memory 4G \
--num-executors 2 \
--class com.project.spark.driver.SparkDriver /home/hadoop/spark.jar \
-c hdfs://localhost:8020/user/hadoop/sparkjob/test-customer.txt \
-s hdfs://localhost:8020/user/hadoop/sparkjob/test-sales.txt \
-o hdfs://localhost:8020/user/hadoop/sparkjob/output \
-state AL,AK

