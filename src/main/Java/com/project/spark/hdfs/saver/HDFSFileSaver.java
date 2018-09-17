package com.project.spark.hdfs.saver;

import org.apache.spark.api.java.JavaPairRDD;

import com.project.spark.properties.Properties;
/**
 * Class to be used for dealing with hdfs file saving operations
 * @author rsuresh
 *
 */
public class HDFSFileSaver {
    
    public void saveToFS(JavaPairRDD<String, String> rdd, String outputFileLocation){
    		if(outputFileLocation.isEmpty()) {
    			outputFileLocation = Properties.OUTPUT_HDFS_PATH;
    		}
        rdd.values().saveAsTextFile(outputFileLocation);
    }
}
