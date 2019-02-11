package ship.hotspot;



import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.SparkConf;
import scala.Tuple2;



public class Test {

	public static void main(String[] args) throws IOException {
		
//Read the long,lat values of the bounding box we want to analyze as well as the starting and ending dates

  		ArrayList<Tuple2<String, Double>> getisOrdListMod = new ArrayList<Tuple2<String, Double>>();
  		getisOrdListMod.add(new Tuple2<String, Double> ("5,4,3",-23.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("5,4,3",6.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("5,4,3",23.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("5,4,3",6.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("1,2,3",3.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("1,2,3",-22.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("2,8,9",-3.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("7,8,9",-5.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("2,5,1",6.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("0,1,2",6.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("2,4,4",6.3));
  		getisOrdListMod.add(new Tuple2<String, Double> ("1,1,1",6.3));
  		
  		
	    for (int i =0;i<getisOrdListMod.size();i++){
	    	
		       System.out.println(getisOrdListMod.get(i));
		    }
	    ArrayList<Tuple2<String, Double>> getisOrdList = new ArrayList<Tuple2<String, Double>>(getisOrdListMod); 
	    
	    Collections.sort(getisOrdList, new Comparator<Tuple2<String, Double>>() {
	    	  public int compare(Tuple2<String, Double> c1, Tuple2<String, Double> c2) {
	    	    if (c1._2 > c2._2) return -1;
	    	    if (c1._2 < c2._2) return 1;
	    	    return 0;
	    	  }});
	    
	    ArrayList<Tuple2<String, Double>> hola = new ArrayList<Tuple2<String, Double>>();
	    hola.add(new Tuple2<String, Double> ("0,0,0",-3.3));
	    System.out.println("create"+hola.size());
	    int size=0;
	    
	    for (int m =0;m<getisOrdList.size();m++){
	    	size=hola.size();
	    	System.out.println("for"+hola.size());
		    if  (size<5) { 
		    	System.out.println("if"+hola.size());
		    	int contains=0;
		    	for (int k=0;k<size;k++) {
		    		
		    		if (getisOrdList.get(m)._1.split(",")[0].equals(hola.get(k)._1.split(",")[0])
		    				&& getisOrdList.get(m)._1.split(",")[1].equals(hola.get(k)._1.split(",")[1])) {
		    			contains = 1;
		    		}
		    	}
	    		if(contains==0) {
	    			hola.add(getisOrdList.get(m));
	    		}
		    }
	    	System.out.println("getis    "+getisOrdList.get(m));
	    }
	    hola.remove(0);
	    for (int holac =0;holac<hola.size();holac++){
	    	System.out.println("hola    "+hola.get(holac));
	    }
	    
	}
}