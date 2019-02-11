package ship.hotspot;

import java.util.Comparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//Finds the minimum and maximum value of the Longitude column
public class Longitude {

	public static void main(String[] args) {


		long start = System.currentTimeMillis();

		SparkConf conf = new SparkConf().setAppName("Longitude");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> lines = sc.textFile("file:///home/user/Downloads/split_imis_3yearsaa");
		


//	    class MinMax implements Comparator<Double>, Serializable {
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public int compare(Double a, Double b) {
//				return 0 ;
//			}
//	    }
	    
	    JavaRDD<Double> longitude = lines.map(v1 -> Double.parseDouble(v1.split(" ")[2]));
	    
	    
	    double max = longitude.max(Comparator.naturalOrder());
	    
	    double min = longitude.min(Comparator.naturalOrder());
	    
		sc.close();
		
		
		System.out.println("------------------------------- Minimum Longitude : " +min+"---------------------------" );
		System.out.println("------------------------------- Maximum Longitude : " +max+"---------------------------" );
		
		
		
		long end = System.currentTimeMillis();



		
		System.out.println("-------------------------------Time to complete : " +(end-start)+"---------------------------" );

	}

}
