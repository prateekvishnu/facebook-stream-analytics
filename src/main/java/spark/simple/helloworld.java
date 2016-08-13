package spark.simple;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class helloworld {
	
	private static final FlatMapFunction<String, String> WORDS_EXTRACTOR =
            new FlatMapFunction<String, String>() {
                public Iterable<String> call(String s) throws Exception {
                    return Arrays.asList(s.split(" "));
                }
            };

    private static final PairFunction<String, String, Integer> WORDS_MAPPER =
            new PairFunction<String, String, Integer>() {
                public Tuple2<String, Integer> call(String s) throws Exception {
                    return new Tuple2<String, Integer>(s, 1);
                }
            };

    private static final Function2<Integer, Integer, Integer> WORDS_REDUCER =
           new Function2<Integer, Integer, Integer>() {
                public Integer call(Integer a, Integer b) throws Exception {
                    return a + b;
                }
            };

    private static final java.lang.String IN_FILE = "C:\\Users\\eashcra\\Documents\\Workspace\\spark.simple\\src\\main\\resources\\input\\input.txt";
    private static final java.lang.String OUT_FILE = "C:\\Users\\eashcra\\Documents\\Workspace\\spark.simple\\src\\main\\resources\\output\\out.txt";

    public static void main(String[] args) throws InterruptedException {

            SparkConf conf = new SparkConf().setAppName("org.sparkexample.WordCount").setMaster("local[2]");
            JavaSparkContext context = new JavaSparkContext(conf);


            JavaRDD<String> file = context.textFile(IN_FILE);
            JavaRDD<String> words = file.flatMap(WORDS_EXTRACTOR);
            JavaPairRDD<String, Integer> pairs = words.mapToPair(WORDS_MAPPER);
            JavaPairRDD<String, Integer> counter = pairs.reduceByKey(WORDS_REDUCER);

            counter.saveAsTextFile(OUT_FILE);
        }


}
