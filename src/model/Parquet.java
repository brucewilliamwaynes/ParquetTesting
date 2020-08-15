package model;

import java.util.List;

import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
/**
 * This class is used to represent Parquet storage in model object format
 * Contains : data : Collection of simpleGroup objects holding the value
 * 			  schema : The schema and metadata information for the given parquet
 * @author 1022773
 *
 */
public class Parquet {
	private List<SimpleGroup> data;
    private List<Type> schema;

    public Parquet(List<SimpleGroup> data, List<Type> schema) {
        this.data = data;
        this.schema = schema;
    }

    public List<SimpleGroup> getData() {
        return data;
    }

    public List<Type> getSchema() {
        return schema;
    }
    
    /**
     * Converting parquet to string
     */
    public String toString() {
    	StringBuilder outputString = new StringBuilder();
    	outputString.append("SCHEMA : ");
		outputString.append("/n");
    	for(Type type : this.schema) {
    		outputString.append(type.toString());
    		outputString.append("/n");
    	}
    	outputString.append("DATA : ");
		outputString.append("/n");
		
    	for(SimpleGroup sgp : this.data) {
    		outputString.append(sgp.toString());
    		outputString.append("/n");
    	}
    	return outputString.toString();
    }
    
}
