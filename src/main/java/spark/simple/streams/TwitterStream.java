package spark.simple.streams;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

public class TwitterStream implements Stream {

	private JavaSparkContext context;
	public TwitterStream(JavaSparkContext context) {
		this.context = context;
	}
	
	public JavaDStream<String> get() {
		// TODO Auto-generated method stub
		return null;
	}

	public void start() {
		
	}
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	public Stream setDuration(long duration) {
		// TODO Auto-generated method stub
		return null;
	}

	public Stream setLocation(String path) {
		// TODO Auto-generated method stub
		return null;
	}

}
