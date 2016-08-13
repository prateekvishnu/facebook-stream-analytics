package spark.simple.streams;

import org.apache.spark.streaming.api.java.JavaDStream;

public interface Stream {

	public JavaDStream<String> get();
	public void start();
	public void stop();
	public Stream setDuration(long duration);
	public Stream setLocation(String path);
	
}
