package spark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;


public class PracticeSparks {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf();
		conf.setAppName("LearningSparkMapReduceParadigm");
		conf.setMaster("local[2]");
		conf.set("es.index.auto.create", "true");
		
		JavaSparkContext sc = new JavaSparkContext(conf);
		/*ArrayList<Tweet> list = new ArrayList<Tweet>();
		list.add(new Tweet("101", "Hell world 1"));
		list.add(new Tweet("102", "Hell world 2"));
		list.add(new Tweet("103", "Hell world 3"));
		JavaRDD<Tweet> jRdd = sc.parallelize(list);
		
		JavaEsSpark.saveToEs(jRdd, "tweets/tweet");
		sc.close();
		*/
		
		
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

		DataFrame df = sqlContext.read().json("examples/src/main/resources/people.json");

		// Displays th	e content of the DataFrame to stdout
		df.show();
	}
}
