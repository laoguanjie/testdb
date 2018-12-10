package test.db;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;

@Data
public class Schema {
	
	private volatile boolean ready = false;
	
	private DDLSql sql;

	// columnName -> columnType
	private Map<String, String> columns = new ConcurrentHashMap<>();
	
	// indexName(columnName + "_id") -> columnName
	private Map<String, String> indices = new ConcurrentHashMap<>();
	
	public Schema copy() {
		Schema s = new Schema();
		Map<String, String> c = new ConcurrentHashMap<>(columns);
		Map<String, String> i = new ConcurrentHashMap<>(indices);
		s.getColumns().putAll(c);
		s.getIndices().putAll(i);
		
		return s;
	}
	
}
