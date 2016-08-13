package spark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import spark.simple.transformers.LabelTransformer;

public class Analytics {

	private static final String APP_NAME = "SocialMediaAnalytics";
	private static final String MASTER_NAME = "local[2]";
	
	private JavaSparkContext jsc;
	private SQLContext sqlCtx;
	
	public Analytics() {
		SparkConf conf = new SparkConf();
		conf.setAppName(APP_NAME);
		conf.setMaster(MASTER_NAME);
		conf.set("es-nodes", "localhost:9200");
		conf.set("es.index.auto.create", "true");
		
		this.jsc = new JavaSparkContext(conf);
		this.sqlCtx = new SQLContext(this.jsc);
		
	}
	
	public JavaSparkContext getSparkContext() {
		return this.jsc;
	}
	
	public SQLContext getSQLContext() {
		return this.sqlCtx;
	}
	
	public PipelineModel train(String location) {
		DataFrame data = getSQLContext().read().json(location);
		
		LabelTransformer labelTransformer = new LabelTransformer();
		data = labelTransformer.transform(data);
	
		Tokenizer tokenizer = new Tokenizer();
		tokenizer.setInputCol("text").setOutputCol("words");
		
		HashingTF hashingTF = new HashingTF()
				.setNumFeatures(1000)
				.setInputCol(tokenizer.getOutputCol())
				.setOutputCol("features");
		
		LogisticRegression lr = new LogisticRegression()
				.setMaxIter(100)
				.setRegParam(0.01);
		
		OneVsRest ovr = new OneVsRest()
				.setClassifier(lr);
		
		IndexToString labelConverter = new IndexToString()
		        .setInputCol("prediction")
		        .setOutputCol("predictedLabel")
		        .setLabels(new String[] {"Customer Service", "Network", "Promotions", "Others"});
		
		Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[]{tokenizer, hashingTF, ovr, labelConverter});
		
		PipelineModel model = pipeline.fit(data);
		
		return model;
	}
}
