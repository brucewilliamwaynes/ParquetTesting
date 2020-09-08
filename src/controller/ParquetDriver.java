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

  public static void main(String[] args) throws Exception {
    String inputURL = "/Users/1022773/Desktop/masterdata.parquet";
    String outputURL = "/Users/1022773/Desktop/masterdata.text";

    List<Long> listOfValues = new ArrayList<Long>();
    listOfValues.add((long) 2767205770L);
    listOfValues.add((long) 2767205778L);
    Parquet filteredParquet = ParquetReaderUtils.getFilteredParquet(inputURL, "PP_PUG_ID", listOfValues);
//  Parquet filteredParquet = ParquetReaderUtils.getFilteredParquet(inputURL, columnName1, columnName2, columnValues1<Long>, columnValues2<Long>);
//  Parquet filteredParquet = ParquetReaderUtils.getFilteredParquetFromStringValues(inputURL, columnName1, columnName2, columnValues1<String>,
//  columnValues2<String>);
    ParquetPublisherUtils.storeParquetData(filteredParquet, outputURL);
  }

}
