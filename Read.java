package ship.hotspot;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Read {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		BufferedReader bf = new BufferedReader(new FileReader("D:\\split_imis_3yearsaa"));
		while ((bf.readLine()) != null){

			String sdx = bf.readLine().split(" ")[2];
			String sdy = bf.readLine().split(" ")[3];
			String sdt = bf.readLine().split(" ")[0];
		
			double dx = Double.parseDouble(sdx);
			double dy = Double.parseDouble(sdy);
			int dt = Integer.parseInt(sdt);
			System.out.println(dx+"--------"+dy+"--------"+dt+"-------");
	    }
		bf.close();
	}

}
