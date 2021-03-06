package test.db;

import java.util.Arrays;
import java.util.List;

import lombok.Data;

@Data
public class Config {
	
	private Base base = new Base();
	
	private Database database = new Database();
	
	@Data
	public static class Database {
		private String jdbcUrl = "jdbc:mysql://localhost:4000/test?useUnicode=true&characterEncoding=UTF-8";
		private String username = "root";
		private String password = "123";
		private String tableName = "testdb";
		private List<String> columnNames = Arrays.asList("name1","type1","time1");
		
	}

	@Data
	public static class Base {
		private int ddlThreads = 400;
		private int insertThreads = 400;
		private long executeSeconds = 30;
	}
}
