package test.db;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.Data;

@Data
public class Sql {
	
	private String query;
	private Map<String, String> variables = new LinkedHashMap<>();
}
