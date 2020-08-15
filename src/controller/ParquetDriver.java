package controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import inputoutput.ParquetPublisherUtils;
import inputoutput.ParquetReaderUtils;
import model.Parquet;
/**
 * Driver class to trigger the application
 * change the inputURL, outputURL for different parquet
 * @author 1022773
 *
 */
public class ParquetDriver {

	public static void main(String[] args) throws IOException {
//		String inputURL = "/Users/1022773/Documents/LRR/sprintRelatedDocuments/resources/OOPT-OGT-305.01/procurementcalendartimes.parquet";
		String inputURL = "/Users/1022773/Desktop/masterdata.parquet";
		String outputURL = "/Users/1022773/Desktop/masterdata.text";
//		List<String> listOfColumns = new ArrayList<String>();
//		listOfColumns.add("item");
//		listOfColumns.add("qty");
//		Parquet processskuParquetColumn = ParquetReaderUtils.getColumnSpecificParquetDataWithoutJar(inputURL, listOfColumns);
//		Parquet processskuParquet = ParquetReaderUtils.getParquetData(inputURL);
//		Parquet processskuFilteredParquet = ParquetReaderUtils.getFilteredParquetData(inputURL,"P_EXTERNAL_CODE","OOPT-DCR100.01-1");
//		Parquet processskuFilteredParquet = ParquetReaderUtils.getFilteredParquetUsingOptions(inputURL,"P_EXTERNAL_CODE","OOPT-DCR100.01-1");
		List<Long> listOfValues = new ArrayList<Long>();
		listOfValues.add((long)2767205770L);
		listOfValues.add((long)2767205772L);
		Parquet processskuFilteredParquet = ParquetReaderUtils.<Long>getFilteredParquetUsingOptions(inputURL,"PP_P_ID", listOfValues);
		ParquetPublisherUtils.storeParquetData(processskuFilteredParquet, outputURL);
	}

}
