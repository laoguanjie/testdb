package db;

import java.util.ArrayList;
import java.util.List;

import test.db.ColumnType;
import test.db.ExceptionType;
import test.db.Schema;
import test.db.Sql;

public class TestException {

	public static void main(String[] args) {
//		System.out.println(ExceptionType.isOutOfRange("6433099230307034030", ColumnType.findByName("float")));
		Sql sql = new Sql();
		sql.getVariables().put("type", "0.7613566573184376");
		sql.getVariables().put("name", "6433099230307034030");
		sql.getVariables().put("time", "A4328=0=:;4?C944");
		List<Schema> schemaList = new ArrayList<>();
		Schema s = new Schema();
//		System.out.println(ExceptionType.OUT_OF_RANGE.verify(sql, latestSchemaList, namedGroups));
	}
}
