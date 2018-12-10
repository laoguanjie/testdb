package test.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSql {
	
	private final List<String> columnList;

	private final String tableTemplate = "create table {table} (`id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id', {columns} PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8";
	
	private final String insertTemplate = "insert into {table} ({keys}) values ({values})";
	
	private final String findByIdTemplate = "select id from {table} where id = {id}";
	
	private final String tableName;

	public RandomSql(String tableName, List<String> columnList) {
		this.tableName = tableName;
		this.columnList = columnList;
	}

	public Map<String, String> randomKT() {
		Map<String, String> kt = new HashMap<>();
		for (String k : columnList) {
			kt.put(k, randomColumnTypeName());
		}

		return kt;
	}

	public CreateTableSql createTable() {
		
		CreateTableSql sql = new CreateTableSql();

		Map<String, String> kt = randomKT();
		StringBuilder columnsBuilder = new StringBuilder("");
		for (Map.Entry<String, String> entry : kt.entrySet()) {
			sql.getVariables().put(entry.getKey(), entry.getValue());
			String column = "`" + entry.getKey() + "` " + entry.getValue() + ",";
			columnsBuilder.append(column);
			if(System.currentTimeMillis()%3 == 1) {
				break;
			}
		}

		String query = tableTemplate.replace("{table}", tableName).replace("{columns}", columnsBuilder.toString());
		sql.setQuery(query);
		
		return sql;
	}

	public DDLSql randomDDL() {
		
		DDLSql sql = new DDLSql();
		DDLOperation[] ddlOperations = DDLOperation.values();

		String key = columnList.get(ThreadLocalRandom.current().nextInt(columnList.size()));

		String type = ColumnType.values()[ThreadLocalRandom.current().nextInt(ColumnType.values().length)]
				.getDefaultName();
		
		sql.getVariables().put(key, type);
		
		DDLOperation operation = ddlOperations[ThreadLocalRandom.current().nextInt(ddlOperations.length)];
		sql.setDdlOperation(operation);
		
		String query = operation.sql(tableName, key, type);
		sql.setQuery(query);
		
		return sql;
	}

	public Sql randomInsert() {
		
		Sql sql = new Sql();

		List<String> keyList = new ArrayList<>(columnList);
		Collections.shuffle(keyList);
		int size = ThreadLocalRandom.current().nextInt(keyList.size()) + 1;
		
		for(String k : keyList.subList(0, size)) {
			sql.getVariables().put(k, randomValue());
		}
		
		String query = insertTemplate.replace("{table}", tableName).replace("{keys}", String.join(",", sql.getVariables().keySet())).replace("{values}", String.join(",", sql.getVariables().values()));
		sql.setQuery(query);
		
		return sql;
	}
	
	private String randomValue() {
		ColumnType[] arr = ColumnType.values();
		return arr[ThreadLocalRandom.current().nextInt(arr.length)].randomValue();
	}
	
	private String randomColumnTypeName() {
		ColumnType[] arr = ColumnType.values();
		return arr[ThreadLocalRandom.current().nextInt(arr.length)].getDefaultName();
	}
	
	public Sql findById(Long id) {
		Sql sql = new Sql();
		String query = findByIdTemplate.replace("{table}", tableName).replace("{id}", id.toString());
		sql.setQuery(query);
		return sql;
	}

}
