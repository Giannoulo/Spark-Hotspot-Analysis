package ship.hotspot;

import java.util.Comparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// Finds the minimum and maximum value of the Latitude column
public class Latitude {

	public static void main(String[] args) {


		long start = System.currentTimeMillis();

		SparkConf conf = new SparkConf().setAppName("Latitude");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("file:///home/user/Downloads/split_imis_3yearsaa");
		
	    
	    JavaRDD<Double> longitude = lines.map(v1 -> Double.parseDouble(v1.split(" ")[3]));
	    
	    
	    double max = longitude.max(Comparator.naturalOrder());
	    
	    double min = longitude.min(Comparator.naturalOrder());
	    
		sc.close();
		
		
		System.out.println("------------------------------- Minimum Latitude : " +min+"---------------------------" );
		System.out.println("------------------------------- Maximum Latitude : " +max+"---------------------------" );
		
		
		
		long end = System.currentTimeMillis();



		
		System.out.println("-------------------------------Time to complete : " +(end-start)+"---------------------------" );

	}

}