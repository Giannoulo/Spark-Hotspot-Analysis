package ship.hotspot;


import java.awt.geom.Point2D;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
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



public class Speed {

	public static void main(String[] args) throws IOException {
		
		//Read the long,lat values of the bounding box we want to analyze as well as the starting and ending dates
		
		Scanner sc1 = new Scanner(System.in);
		
		System.out.println("Please enter the initial longitude value for the bounding box: ");
		double xi = sc1.nextDouble();
		
		System.out.println("Please enter the final longitude value for the bounding box: ");
		double xf = sc1.nextDouble();
		
		System.out.println("Please enter the initial latitude value for the bounding box: ");
		double yi = sc1.nextDouble();
		
		System.out.println("Please enter the final latitude value for the bounding box: ");
		double yf = sc1.nextDouble();
		
		System.out.println("Please enter the start date in UNIX timestamp: ");
		int ti = sc1.nextInt();
		
		System.out.println("Please enter the end date in UNIX timestamp: ");
		int tf = sc1.nextInt();
		
		System.out.println("Please enter the number of the Top-k cells: ");
		int topk = sc1.nextInt();
		
				
		//The width of the bounding box
		
		double boundWidth = Math.abs(xf-xi);
		
		//The Duration of the time window
		int boundHeight = Math.abs(tf-ti);
		

		//The number of the cells on the horizontal axis on the 2D plane. The shape of the cells is that of a square
		
		System.out.println("PLease enter the number of the horizontal cells on the 2D plane: ");
		int nHorCells = sc1.nextInt();
		
		//The number of the cells on the vertical axis on the 3D plane. The shape of the cells is that of a cube	
		System.out.println("PLease enter the number of the vertical cells: ");
		int nTimeCells = sc1.nextInt();
		
		sc1.close();
		
  		long start = System.currentTimeMillis();
  		
		//The division of the bounding width with the number of the cells will return the width of each 
		//cell which is also the height of the cell because they are squares. Divided by 2 produces the 
		//distance of the centroid of each cell and the boundaries of that cell	
		
		double cDist = (boundWidth/nHorCells)/2;
		
		//The time distance from the boundaries of each cube
		int cDuration = (boundHeight/nTimeCells)/2;
		
		

		//Create a list with the coordinates of the centroids of each cell and a list with the timestamps of each cell
		
		List<Integer> centroidsTime = new ArrayList<Integer>();
		for (int t = ti + cDuration; t < tf; t = t + (2*cDuration)) {
			centroidsTime.add(t);
		}
		
		
		
		List<Point2D.Double> centroidsCoo = new ArrayList<Point2D.Double>();
		for (double m = yi + cDist; m < yf; m = m + (2*cDist)) {
			for (double k = xi + cDist; k < xf; k = k + (2*cDist)) {
				centroidsCoo.add(new Point2D.Double(k,m));
			}
		}
		
		//Update the coordinates of the outer layers of the bounding box to represent the coordinates of the outer cells centroids
		xi=xi + cDist;
		xf=xf - cDist;
		ti=ti + cDuration;
		tf=tf - cDuration;
		yf=yi+((int)(Math.abs(yf-yi)/(2*cDist)))*(2*cDist);
		yi=yi + cDist;


		//Create the SparkConf and the JavaSparkContext objects
		SparkConf conf = new SparkConf().setAppName("HotSpots");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		//Broadcast the centroids points list, brDist and brDuration so that they can be accessible from all the workers
		Broadcast<List<Point2D.Double>> brCoo = sc.broadcast(centroidsCoo);
		Broadcast<List<Integer>> brTime = sc.broadcast(centroidsTime);
		Broadcast<Double> brDist = sc.broadcast(cDist);
		Broadcast<Integer> brDuration = sc.broadcast(cDuration);
		Broadcast<Double> brxi = sc.broadcast(xi);
		Broadcast<Double> brxf = sc.broadcast(xf);
		Broadcast<Double> bryi = sc.broadcast(yi);
		Broadcast<Double> bryf = sc.broadcast(yf);
		Broadcast<Integer> brti = sc.broadcast(ti);
		Broadcast<Integer> brtf = sc.broadcast(tf);
		
		//Read the input data
		JavaRDD<String> lines = sc.textFile("file:/home/user/imis");
		
			
	
		//Create a JavaPairRDD with tuple elements. For each String line of lines we split the string 
		//and assign latitude, longitude and timestamp of each line to sdx,sdy and sdt. Then we check if the data point of 
		//that line is contained in a cell of the centroids list. If it is then a new tuple is returned
		//with key the latitude, Longitude and timestamp (split by ",") of that cell and value 1.

		JavaPairRDD<String, Integer> pairs = lines.mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, String, Integer>) x -> {
			
			ArrayList<Tuple2<String, Integer>> pairList = new ArrayList<Tuple2<String, Integer>>();
			
			//Get the needed broadcasted variables
			double dist = brDist.getValue();
			int dur = brDuration.getValue();
			
			double pxi = brxi.getValue();
			double pxf = brxf.getValue();
			double pyi = bryi.getValue();
			double pyf = bryf.getValue();
			int pti = brti.getValue();
			int ptf = brtf.getValue();
			
			List<Integer> t = brTime.getValue();
			List<Point2D.Double> p = brCoo.getValue();
			
			while (x.hasNext()) {
				
					String line = x.next();
					String sdx = line.split(" ")[2];
					String sdy = line.split(" ")[3];
					String sdt = line.split(" ")[0];
				
					//Parse as numbers in order to compare them to the coordinates
					double dx = Double.parseDouble(sdx);
					double dy = Double.parseDouble(sdy);
					int dt = Integer.parseInt(sdt);

					//Check if the point is enclosed in a cell from the list	
					if (dx>=pxi && dx<=pxf && dy>=pyi && dy<=pyf && dt>=pti && dt<=ptf) {
						for(int timeCounter=0; timeCounter<t.size(); timeCounter++) {
					    	for ( int cooCounter=0; cooCounter < p.size(); cooCounter++) {
					    		
					    		//Centroids coordinates
						    	double cx = p.get(cooCounter).getX();
						    	double cy = p.get(cooCounter).getY();
						    	int ct = t.get(timeCounter);
						    	
						    	if (dx > (cx-dist) && dx <= (cx+dist)) {
						    		if (dy > (cy-dist) && dy <= (cy+dist)) {
						    			if (dt > (ct-dur) && dt <= (ct+dur)) {
						    				
						    				String scx = Double.toString(cx);
							    			String scy = Double.toString(cy);
							    			String sct = Integer.toString(ct);
							    			
							    			pairList.add(new Tuple2<String, Integer>(scx+","+scy+","+sct,1));
						    			}
						    		}
						    	}
						    }
					    }
					}
				}
				return pairList.iterator();
		});
		
		
		//The number of the cells that are created	
		int numberOfCells = brCoo.getValue().size()*brTime.getValue().size();
			
		System.out.println("-------------------------Number of cells: "+numberOfCells+" -------------------------");
		
		//The filename of the txt file to save the output
		String filename = "imis-lon"+Double.toString(xi)+","+
				Double.toString(xf)+",lat"+
				Double.toString(yi)+","+
				Double.toString(yf)+",time"+
				Integer.toString(ti)+","+
				Integer.toString(tf)+",NCells"+numberOfCells;

		
		//ReduceByKey the pairs RDD to see how many points are contained in each cell of the grid
		
		JavaPairRDD<String, Integer> cellCounts = pairs.reduceByKey((a,b) -> a+b);

		//Calculate the cell average and standard deviation of the cells. To be used in the Getis Ord equation
		double cellAverage = (cellCounts.values().reduce((a,b) -> a+b)/numberOfCells);
		
		double sDeviation = Math.sqrt((cellCounts.values().map(a -> Math.pow(a, 2))
				.reduce((a,b) -> a+b)/numberOfCells) - Math.pow(cellAverage, 2));
		
		//Broadcast the variables for the workers
		Broadcast<Double> brCellAverage = sc.broadcast(cellAverage);
		Broadcast<Double> brSDeviation = sc.broadcast(sDeviation);
		Broadcast<Integer> brNumberOfCells = sc.broadcast(numberOfCells);
		
		
		System.out.println("--------- Average:  "+cellAverage);
		System.out.println("--------- Deviation:  "+sDeviation);
		
		
		//Collect the rdd to the driver. The list contains only the cells that have a count
		List<Tuple2<String, Integer>> cellList = cellCounts.collect();

		System.out.println("------------- Cell Counts:   "+cellList.size());
		
		
		//Construct the grid with the empty cells as well		
		List<Tuple2<String, Integer>> grid = new ArrayList<Tuple2<String, Integer>>();
		
		int contains=0;
		//Check if the cell is contained in the cellList and if it is add it to the grid list, if not add a cell with a zero count
		for (int n=0; n<centroidsTime.size();n++) {
			for (int m=0; m<centroidsCoo.size();m++) {
				
				contains = 0;
				
				String a = Double.toString(centroidsCoo.get(m).getX());
				String b = Double.toString(centroidsCoo.get(m).getY());
				String c = Integer.toString(centroidsTime.get(n));
				
				for (int k=0; k<cellList.size();k++) {
					
					if (cellList.get(k)._1.equals(a+","+b+","+c)){

						contains = 1;
						grid.add(new Tuple2<String, Integer>(a+","+b+","+c,cellList.get(k)._2));
						break;
					}
				}
				
				if (contains == 0){

					grid.add(new Tuple2<String, Integer>(a+","+b+","+c,0));
				}
			}
		}

		System.out.println("---------------- Grid Size:   "+grid.size());
		
		//Save the grid with the cell counts
  		File fileCounts = new File(filename+"counts.txt");

	    FileOutputStream foC = new FileOutputStream(fileCounts);
	    
	    PrintWriter pwC = new PrintWriter(foC);
	    
	    for (int i =0;i<grid.size();i++){
	    	
		       pwC.println(grid.get(i)._1+","+grid.get(i)._2());
		    }
		    pwC.close();
		    foC.close();
		
		//Create a RDD from the grid list
		JavaPairRDD<String, Integer> gridRDD = sc.parallelizePairs(grid);
	    
		//For each cell a in the gridrdd find the neighboring cells and create a tuple for each one of them with 
		//its name as key and the count of a as value
	    JavaPairRDD<String, Integer> neighborRDD = gridRDD.flatMapToPair((PairFlatMapFunction<Tuple2<String, Integer>, String, Integer>) a -> {
	    	
	    	List<Tuple2<String, Integer>> neighbors = new ArrayList<Tuple2<String, Integer>>();
	    	
			String slat = a._1.split(",")[1];
			String slon = a._1.split(",")[0];
			String stime = a._1.split(",")[2];
			
			double lat = Double.parseDouble(slat);
			double lon = Double.parseDouble(slon);
			int time = Integer.parseInt(stime);
			
			double distance = brDist.getValue();
			int duration = brDuration.getValue();
			
			//Calculate the coordinates of the neighboring cells
			double latMinus = lat-(2*distance);
			double latPlus = lat+(2*distance);
			double lonMinus = lon-(2*distance);
			double lonPlus = lon+(2*distance);
			int timeMinus = time-(2*duration);
			int timePlus = time+(2*duration);
			
			
			List<Double> latList = Arrays.asList(lat, latMinus, latPlus);
			List<Double> lonList = Arrays.asList(lon, lonMinus, lonPlus);
			List<Integer> timeList = Arrays.asList(time, timeMinus, timePlus);
			
			//Get the broadcasted variables
			double nxi=brxi.getValue();
			double nxf=brxf.getValue();
			double nyi=bryi.getValue(); 
			double nyf=bryf.getValue();
			int nti=brti.getValue();
			int ntf=brtf.getValue();
			
			//Check each neighbor cell if its within the bounding box and if its not the same cell as the center cell.
			//If its OK a tuple is returned 
			for (int i=0;i<timeList.size();i++) {
				for (int k=0;k<latList.size();k++) {
					for (int m=0;m<lonList.size();m++) {
						if (!(latList.get(k)==lat && lonList.get(m)==lon && timeList.get(i)==time)) {
							if(latList.get(k)>=nyi &&
									latList.get(k)<=nyf &&
									lonList.get(m)>=nxi && 
									lonList.get(m)<=nxf &&
									timeList.get(i)>=nti && 
									timeList.get(i)<=ntf){
								
								neighbors.add(new Tuple2<String, Integer>(Double.toString(lonList.get(m))+","+
								Double.toString(latList.get(k))+","+Integer.toString(timeList.get(i)),a._2));
							}
						}
					}
				}
			}
			//Reduce by key on the neighbor cells 
	    	return  neighbors.iterator();}).reduceByKey((a,b) -> a+b);
	    
	    //Collect the neighbor rdd to the driver as a list to see the result
  		List<Tuple2<String, Integer>> neighborList = neighborRDD.collect();
  		System.out.println("------------- Neighbor Cells :   "+neighborList.size());
  		
	    
	    //Calculate the getis Ord statistic for each cell of the grid
	    JavaPairRDD<String, Double> getisOrdRDD = neighborRDD.mapToPair((PairFunction<Tuple2<String, Integer>, String, Double>) a -> {
	    	
			String slat = a._1.split(",")[1];
			String slon = a._1.split(",")[0];
			String stime = a._1.split(",")[2];
			
			int sum = a._2;
			int n=0; //The number of the neighbor cells
			
			//A list with the values that correspond to the cells on the outer layers of the cube.
			ArrayList<String> critical = new ArrayList<String>();
			critical.add(Double.toString(brxi.getValue()));
			critical.add(Double.toString(brxf.getValue()));
			critical.add(Double.toString(bryi.getValue()));
			critical.add(Double.toString(bryf.getValue()));
			critical.add(Integer.toString(brti.getValue()));
			critical.add(Integer.toString(brtf.getValue()));

			//The actual values of the cell			
			ArrayList<String> actual = new ArrayList<String>();
			actual.add(slat);
			actual.add(slon);
			actual.add(stime);
			
			//Keep only the common elements between critical and actual lists
			critical.retainAll(actual);

			//Check if the cell is in the outer layers of the cube to calculate the number of the neighbor cells
			if (critical.size()==0) {
				n=26;
			}
			if (critical.size()==1) {
				n=17;
			}
			if (critical.size()==2) {
				n=11;
			}
			if (critical.size()==3) {
				n=7;
			}
			
			//Calculate the getis Ord for the cell
			double getisOrd = (sum - (brCellAverage.getValue()*n))/(brSDeviation.getValue()*Math.sqrt((brNumberOfCells.getValue()*n - Math.pow(n, 2))/ brNumberOfCells.getValue()-1));
	    	
			return new Tuple2<String,Double>(slon+","+slat+","+stime, getisOrd);
	    });
	    
	    
	    //Collect the rdd to the driver
  		List<Tuple2<String, Double>> getisOrdList = getisOrdRDD.collect();
  		System.out.println("------------- Getis Ord Cells :   "+getisOrdList.size());
  		
  		//Sort the getis ord list
  		ArrayList<Tuple2<String, Double>> getisOrdListMod = new ArrayList<Tuple2<String, Double>>(getisOrdList);
	    Collections.sort(getisOrdListMod, new Comparator<Tuple2<String, Double>>() {
	    	  public int compare(Tuple2<String, Double> c1, Tuple2<String, Double> c2) {
	    	    if (c1._2 > c2._2) return -1;
	    	    if (c1._2 < c2._2) return 1;
	    	    return 0;
	    	  }});
	    
	    
	    //Save the list to a text file
  		File file = new File(filename+".txt");

	    FileOutputStream fo = new FileOutputStream(file);
	    
	    PrintWriter pw = new PrintWriter(fo);
	    
	    for (int i =0;i<getisOrdListMod.size();i++){
	    	
		       pw.println(getisOrdListMod.get(i)._1+","+getisOrdListMod.get(i)._2());
		    }
		    pw.close();
		    fo.close();
	    
		
		//Create a list to keep the k cells with the highest getis ord score
	    ArrayList<Tuple2<String, Double>> uniqueTopK = new ArrayList<Tuple2<String, Double>>();
	    uniqueTopK.add(new Tuple2<String, Double> ("0,0,0",-3.3));

	    for (int m =0;m<getisOrdListMod.size();m++){

		    if  ( uniqueTopK.size() < (topk+1) ) { 

		    	int unique=0;
		    	
		    	for (int k=0;k<uniqueTopK.size();k++) {
		    		
		    		if (getisOrdListMod.get(m)._1.split(",")[0].equals(uniqueTopK.get(k)._1.split(",")[0])
		    				&& getisOrdListMod.get(m)._1.split(",")[1].equals(uniqueTopK.get(k)._1.split(",")[1])) {
		    			unique = 1;
		    		}
		    	}
	    		if(unique==0) {
	    			uniqueTopK.add(getisOrdListMod.get(m));
	    		}
		    }
	    }
	    uniqueTopK.remove(0);
  		
	    
	    //Save the top k cells to a text file
	    File filetop = new File(filename+"-top"+topk+".txt");
	    FileOutputStream fotop = new FileOutputStream(filetop);
	    PrintWriter pwtop = new PrintWriter(fotop);

	    for (int i =0;i<uniqueTopK.size();i++){
	    	
	       pwtop.println(uniqueTopK.get(i)._1+","+uniqueTopK.get(i)._2());
	       System.out.println(uniqueTopK.get(i)._1+","+uniqueTopK.get(i)._2());
	    }
	    
	    pwtop.close();
	    fotop.close();
	    
	    //Print the execution time
  		long end = System.currentTimeMillis();
  		System.out.println("--------------- Execution time is "+((end - start)/60000)+" minutes");
	    
  		//Close the spark context 
		sc.close();
	}
	
}

