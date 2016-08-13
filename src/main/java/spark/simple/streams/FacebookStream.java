/*
 * 
 * 	File: FacebookStream.java
 *  Author: Ashish Chopra
 *  Date: 3 Dec, 2015
 *  --------------------------------------------
 *         
 * 
 */
package spark.simple.streams;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class FacebookStream implements Stream {

	private JavaStreamingContext streamContext;
	private JavaSparkContext sparkContext;
	private SQLContext sqlContext;
	private String location;
	private long duration;
		
	public FacebookStream(JavaSparkContext sparkContext) {
		if (sparkContext == null)
			throw new IllegalArgumentException("sparkContext is not provided.");
		this.sparkContext = sparkContext;
		this.sqlContext = new SQLContext(sparkContext);
	}
	
	public JavaDStream<String> get() {
		if (streamContext == null) 
			throw new NullPointerException("Facebook Streaming not initialized properly.");
		JavaDStream<String> lines = streamContext.textFileStream(this.location);
		return lines;
	}
	
	public void start() {
		streamContext.start();
		streamContext.awaitTermination();
	}
	
	public void stop() {
		if (this.streamContext == null)
			throw new NullPointerException("Facebook Streaming not initialized properly.");
		this.streamContext.stop();
	}
	
	public FacebookStream setDuration(long duration) {
		if (duration <= 0)
			throw new IllegalArgumentException("duration is non-positive value.");
		this.duration = duration;
		this.streamContext = new JavaStreamingContext(sparkContext, new Duration(duration));
		return this;
	}
	
	public FacebookStream setLocation(String location) {
		if (duration <= 0)
			throw new IllegalArgumentException("location not specified");
		this.location = location;
		return this;
	}
	
	
}