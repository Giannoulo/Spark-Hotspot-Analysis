package ship.hotspot;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//Finds the number of rows in the dataset
public class Count {

	public static void main(String[] args) {

		long start = System.currentTimeMillis();

		SparkConf conf = new SparkConf().setAppName("Count Rows");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("file:/home/user/imis");
		

		long linesNumber = lines.count();
		
		sc.close();
		System.out.println("------------------------------- Number of Lines : " +linesNumber+"---------------------------" );
		long end = System.currentTimeMillis();
		
		System.out.println("-------------------------------Time to complete : " +(end-start)+"---------------------------" );
	}

}
