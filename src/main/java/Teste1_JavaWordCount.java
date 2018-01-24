import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

public class Teste1_JavaWordCount {

	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) {

		SparkSession spark = SparkSession
				.builder()
				.appName("JavaWordCount")
				.master("local[*]")
				.getOrCreate();

		String textSample = Teste1_JavaWordCount.class.getClassLoader().getResource("bible.txt").getPath();

		JavaRDD<String> lines = spark.read().textFile(textSample).javaRDD();

		JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());

		JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));

		JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);

		List<Tuple2<String, Integer>> output = counts.collect();

		output
				.parallelStream()
				.sorted(Comparator.comparingInt(Tuple2<String, Integer>::_2))
				.forEachOrdered(tuple ->{
					System.out.println(tuple._1() + ": " + tuple._2());
				});


		spark.stop();
	}
}