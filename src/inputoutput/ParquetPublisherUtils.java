package inputoutput;

import java.io.FileWriter;
import java.io.IOException;

import model.Parquet;
/**
 * creates a file writer and dumps the Parquet object in destinationURL
 * @author 1022773
 * 
 * To do: For ABS storage
 */
public class ParquetPublisherUtils {
	public static boolean storeParquetData(Parquet inputParquet, String outputURL) throws IOException {
		FileWriter fw = new FileWriter(outputURL);
		fw.write(inputParquet.toString());
		fw.close();
		return true;
	}
	
	public static boolean createJSONFromParquet(Parquet inputParquet, String outputURL) {
	  
	  return true;
	}
	
}
