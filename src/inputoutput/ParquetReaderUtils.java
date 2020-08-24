package inputoutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.filter.ColumnPredicates;
import org.apache.parquet.filter.ColumnPredicates.Predicate;
import org.apache.parquet.filter.ColumnRecordFilter;
import org.apache.parquet.filter.OrRecordFilter;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.filter2.compat.FilterCompat.Visitor;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators.BinaryColumn;
import org.apache.parquet.filter2.predicate.Operators.IntColumn;
import org.apache.parquet.filter2.predicate.Operators.LongColumn;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;

import model.Parquet;
/**
 * This class contains utilities related to Reading of parquet from file path
 * @author 1022773
 * To do : For ABS storage
 */
public class ParquetReaderUtils {
	/**
	 * the method reads the parquet file and builds the Parquet model object
	 * @param filePath
	 * @return Parquet object for the corresponding parquet file
	 * @throws IOException
	 */
	public static Parquet getParquetData(String filePath) throws IOException {
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		ParquetFileReader reader = ParquetFileReader
				.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
		MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		List<Type> fields = schema.getFields();
		PageReadStore pages;
		while ((pages = reader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

			for (int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
				simpleGroups.add(simpleGroup);
			}
		}
		reader.close();
		return new Parquet(simpleGroups, fields);
	}
	
	
	/**
	 * Reads and parquet file and builds Parquet object based on the filter , single columnNamne and columnValue
	 * @param filePath
	 * @param columnName
	 * @param columnValue
	 * @return Parquet
	 * @throws IllegalArgumentException
	 * @throws IOException
	 * 
	 * to do :1. Mutliple column supoprt for filtering and multiple values
	 * 		  2. 'Like' matching for the column
	 */
	public static Parquet getFilteredParquetData(String filePath, String columnName, String columnValue) throws IllegalArgumentException, IOException {
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		ParquetFileReader reader = ParquetFileReader
				.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
		MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		List<Type> fields = schema.getFields();
		PageReadStore pages;
		while ((pages = reader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			UnboundRecordFilter filter = ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(columnValue));
			RecordReader recordFilteredRecords = columnIO.getRecordReader(pages, new GroupRecordConverter(schema), filter);
			for(int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordFilteredRecords.read();
				if(simpleGroup == null) {
					break;
				}
				simpleGroups.add(simpleGroup);
			}
		}		
		reader.close();
		return new Parquet(simpleGroups, fields);
	}
	
	/**
	 * this method uses options of ParquetFileReader and uses FilterPredicate to filter one column value for a given column.
	 * @param filePath
	 * @param columnName
	 * @param columnValue
	 * @return
	 * @throws IOException
	 */
	public static Parquet getFilteredParquetUsingOptions(String filePath, String columnName, String columnValue) throws IOException {
	  FilterPredicate filterPredicate = FilterApi.eq(FilterApi.binaryColumn(columnName), Binary.fromString(columnValue));
	  FilterCompat.Filter recordFilter = FilterCompat.get(filterPredicate);
	  ParquetReadOptions options = ParquetReadOptions.builder().withRecordFilter(recordFilter).build();
	  ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(filePath), new Configuration()), options);
	  List<SimpleGroup> simpleGroups = new ArrayList<>();
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader recordFilteredRecords = columnIO.getRecordReader(pages, new GroupRecordConverter(schema), recordFilter);
          for(int i = 0; i < rows; i++) {
              SimpleGroup simpleGroup = (SimpleGroup) recordFilteredRecords.read();
              if(simpleGroup == null) {
                  break;
              }
              simpleGroups.add(simpleGroup);
          }
      }       
      reader.close();
      return new Parquet(simpleGroups, fields);
	}
	
	/**
	 * This method filters the parquet based on a List of column values for a particular column,
     * using FilterPredicate and options for RecordReader.
     * Using the same technique we can have a list of columns and build our predicates over it.
	 * @param filePath
	 * @param columnName
	 * @param columnValues
	 * @return
	 * @throws Exception
	 * @throws IOException
	 */
	public static Parquet getFilteredParquet(String filePath, String columnName, List<Long> columnValues) throws Exception, IOException {
	  //Type of column we are filtering on.
	  LongColumn column = FilterApi.longColumn(columnName);
	  
	  //building Filter predicate.
	  FilterPredicate finalFilterPredicate = FilterApi.eq(column, columnValues.get(0));
      for(int index = 1; index < columnValues.size(); index++) {
        finalFilterPredicate = FilterApi.or(finalFilterPredicate, FilterApi.eq(column, columnValues.get(index)));
      }
	  
      FilterCompat.Filter recordFilter = FilterCompat.get(finalFilterPredicate);
      
	  ParquetReadOptions options = ParquetReadOptions.builder().withRecordFilter(recordFilter).build();
      
	  //Building ParquetFileReader with Filter.
	  ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(filePath), new Configuration()), options);
      
	  //Reading reader with Filtered Rows and building a parquet.
	  List<SimpleGroup> simpleGroups = new ArrayList<>();
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader recordFilteredRecords = columnIO.getRecordReader(pages, new GroupRecordConverter(schema), recordFilter);
          for(int i = 0; i < rows; i++) {
            SimpleGroup simpleGroup = (SimpleGroup) recordFilteredRecords.read();
            if(simpleGroup != null) {
              simpleGroups.add(simpleGroup);
            }
        }
      }       
      reader.close();
      return new Parquet(simpleGroups, fields);
	}
	
	/**
	 * This method filters the parquet based on a List of column values for a particular column,
	 * using FilterPredicate and options for RecordReader.
	 * Using the same technique we can have a list of columns and build our predicates over it.
	 * @param filePath
	 * @param columnName
	 * @param columnValues
	 * @return
	 * @throws IOException
	 */
	public static Parquet getFilteredParquetUsingOptions(String filePath, String columnName, List<Long> columnValues) throws IOException {
//	  BinaryColumn column = FilterApi.binaryColumn(columnName);
	  UnboundRecordFilter unboundRecordFilter = ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(columnValues.get(0)));
//	  FilterPredicate finalFilterPredicate = FilterApi.eq(column, Binary.fromString(columnValues.get(0)));
	  for(long columnValue : columnValues) {
//	    FilterPredicate filterPredicate = FilterApi.eq(column, Binary.fromString(columnValue));
//	    finalFilterPredicate = FilterApi.or(finalFilterPredicate, filterPredicate);
	    unboundRecordFilter = OrRecordFilter.or(unboundRecordFilter, ColumnRecordFilter.column(columnName, ColumnPredicates.equalTo(columnValue)));
	  }
//      FilterCompat.Filter recordFilter = FilterCompat.get(finalFilterPredicate);
      FilterCompat.Filter unboundRecordFilter1 = FilterCompat.get(unboundRecordFilter);
//      ParquetReadOptions options = ParquetReadOptions.builder().withRecordFilter(recordFilter).build();
      ParquetReadOptions options = ParquetReadOptions.builder().withRecordFilter(unboundRecordFilter1).build();
      ParquetFileReader reader = new ParquetFileReader(HadoopInputFile.fromPath(new Path(filePath), new Configuration()), options);
      List<SimpleGroup> simpleGroups = new ArrayList<>();
      MessageType schema = reader.getFooter().getFileMetaData().getSchema();
      List<Type> fields = schema.getFields();
      PageReadStore pages;
      while ((pages = reader.readNextRowGroup()) != null) {
          long rows = pages.getRowCount();
          MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
          RecordReader recordFilteredRecords = columnIO.getRecordReader(pages, new GroupRecordConverter(schema), unboundRecordFilter1);
          for(int i = 0; i < rows; i++) {
              SimpleGroup simpleGroup = (SimpleGroup) recordFilteredRecords.read();
              if(simpleGroup != null) {
                simpleGroups.add(simpleGroup);
              }
          }
      }
      
      IntColumn column = FilterApi.intColumn(columnName);
      reader.close();
      return new Parquet(simpleGroups, fields);
    }
	
	
	/**
	 * With Map Reduce Jar getting column specific parquet object
	 * 
	 * To do : Includes deprecated method
	 * @param filePath
	 * @param listOfColumns
	 * @return
	 * @throws IOException
	 */
	public static Parquet getColumnSpecificParquetDataWithMapReduceJar(String filePath, List<String> listOfColumns) throws IOException {
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		Configuration conf = new Configuration();
		Path path = new Path(filePath);
		ParquetMetadata metadata = ParquetFileReader.readFooter(conf, path);
		List<ColumnDescriptor> columnsOriginal = metadata.getFileMetaData().getSchema().getColumns();
		List<ColumnDescriptor> columnsFiltered = new ArrayList<ColumnDescriptor>();
		for(ColumnDescriptor colDesc : columnsOriginal) {
			String columnName = colDesc.getPath()[0];
			if(listOfColumns.contains(columnName)) {
				columnsFiltered.add(colDesc);
			}
		}
		ParquetFileReader reader = new ParquetFileReader(conf, path, metadata.getBlocks(), columnsFiltered);
		PageReadStore pages;
		MessageType schema = reader.getFooter().getFileMetaData().getSchema();
		List<Type> fields = schema.getFields();
		reader.setRequestedSchema(schema);
		while ((pages = reader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

			for (int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
				simpleGroups.add(simpleGroup);
			}
		}
		return new Parquet(simpleGroups, fields);
	}
	
	/**
	 * Without jar building column specific parquet data
	 * To do : Finding the required columns can be made faster 
	 * 		   Implement Binary searching for it if possible
	 * @param filePath
	 * @param listOfColumns
	 * @return
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	public static Parquet getColumnSpecificParquetDataWithoutJar(String filePath, List<String> listOfColumns) throws IllegalArgumentException, IOException {
		List<SimpleGroup> simpleGroups = new ArrayList<>();
		ParquetFileReader originalReader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
		ParquetMetadata metadata = originalReader.getFooter();
		MessageType schemaOriginal = metadata.getFileMetaData().getSchema();
		List<Type> fieldsOriginal = schemaOriginal.getFields();
		List<Type> fieldsFiltered = new ArrayList<Type>();
		for(Type type : fieldsOriginal) {
			String valueType = type.getName();
			if(listOfColumns.contains(valueType)) {
				fieldsFiltered.add(type);
			}
		}
		MessageType schema = new MessageType(schemaOriginal.getName(), fieldsFiltered);
		PageReadStore pages;
		originalReader.setRequestedSchema(schema);
		while ((pages = originalReader.readNextRowGroup()) != null) {
			long rows = pages.getRowCount();
			MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
			RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

			for (int i = 0; i < rows; i++) {
				SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
				simpleGroups.add(simpleGroup);
			}
		}
		originalReader.close();
		return new Parquet(simpleGroups, fieldsFiltered);
	}
	
}
