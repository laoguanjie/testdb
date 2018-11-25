package test.db;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;

@Data
public class Schema {

	// columnName -> columnType
	private Map<String, String> columns = new ConcurrentHashMap<>();
	
	// indexName -> columnName
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
