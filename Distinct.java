package ship.hotspot;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//Finds the number of the distinct ids
public class Distinct {

	public static void main(String[] args) {


		long start = System.currentTimeMillis();

		SparkConf conf = new SparkConf().setAppName("Distinct");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("file:///home/user/Downloads/split_imis_3yearsaa");

		
	    JavaRDD<Integer> ids = lines.map(v1 -> Integer.parseInt(v1.split(" ")[1]));
	    
	    JavaRDD<Integer> unique = ids.distinct();
	    

		long distinctNumber = unique.count();
	    
		sc.close();
		
		
		System.out.println("------------------------------- Number of distinct ids : " +distinctNumber+"---------------------------" );
		
		
		
		long end = System.currentTimeMillis();



		
		System.out.println("-------------------------------Time to complete : " +(end-start)+"---------------------------" );

	}

}