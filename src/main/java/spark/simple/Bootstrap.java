package spark.simple;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.elasticsearch.spark.sql.EsSparkSQL;

import spark.simple.streams.FacebookStream;
import spark.simple.streams.Stream;

public class Bootstrap {

	private static final String TRAINING_DATASET = "src/main/resources/datasets/training/training-data.txt";
	private static final String STREAMING_DATASET_LOCATION = "src/main/resources/datasets/streaming";
	
	public static void main(String[] args) {
		
		final Analytics analytics = new Analytics();
		final PipelineModel model = analytics.train(TRAINING_DATASET);
		
		// start a real time stream of 1000 ms duration
		Stream fbStream = new FacebookStream(analytics.getSparkContext())
				.setDuration(10000)
				.setLocation(STREAMING_DATASET_LOCATION);

		JavaDStream<String> fbData = fbStream.get();
	
		fbData.foreachRDD(new Function2<JavaRDD<String>, Time, Void>() {
			public Void call(JavaRDD<String> rdd, Time time) throws Exception {

				SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
				DataFrame data = sqlContext.read().json(rdd);
				data.show();
				
				// run the predictions on the streamed data
				DataFrame output = model.transform(data);	
				output.show();
				
				// writing output to Elastic search for visualizations
				EsSparkSQL.saveToEs(output.select("id", "tag", "lang", "text", "prediction", "predictedLabel", "createdAt"), "fbspark/comments");
				
				return null;
			}
		});
		
		fbStream.start();
	}

}
