package test.db;

import java.util.Collection;
import java.util.Map;

import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.code.regexp.Pattern;

public enum ExceptionType {
	

	COLUMN_NOT_EXIST(Pattern.compile("^Unknown column '(?<columnName>.{1,1000})' in 'field list'")) {

		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			String columnName = namedGroups.get("columnName");

			for (Schema s : latestSchemaList) {
				if(s.getColumns() == null) {
					logger.error("columns is null, schema={}",s);
				}
				if (!s.getColumns().containsKey(columnName)) {
					return true;
				}
			}
			
			return false;
		}
	},
	UNKNOW_COLUMN(Pattern.compile("unknown column (?<columnName>\\w+)")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			String columnName = namedGroups.get("columnName");

			for (Schema s : latestSchemaList) {
				if(s.getColumns() == null) {
					logger.error("columns is null, schema={}",s);
				}
				if (!s.getColumns().containsKey(columnName)) {
					return true;
				}
			}
			
			return false;
		}
	},
	INCORRECT_TIME_FORMAT(Pattern.compile("Data truncation: invalid time format: '(?<val>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			
			if(val.startsWith("{") && val.endsWith("}")) {
				//TODO
				return true;
			} else {
				return INCORRECT_DATETIME.verify(sql, latestSchemaList, namedGroups, schemaLine);
			}
		}
	},
	INCORRECT_DATETIME(Pattern.compile("^Data truncation: Incorrect datetime value: '(?<val>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				LocalDateTime.parse(val, DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss"));
			} catch (Exception e) {
				return true;
			}
			
			if(!val.matches("^\\d{4}-\\d{2}-\\d{2}\\s\\d{2}:\\d{2}:\\d{2}")) {
				return true;
			}
			
			return false;
		}
	},
	INCORRECT_DATE(Pattern.compile("^Data truncation: Incorrect date value: '(?<val>.*)' for column '(?<columnName>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				LocalDateTime.parse(val, DateTimeFormat.forPattern("yyyy-MM-dd"));
			} catch (Exception e) {
				return true;
			}
			
			if(!val.matches("^\\d{4}-\\d{2}-\\d{2}")) {
				return true;
			}
			
			System.out.println("date="+val);
			return false;
		}
	},
	CONVERT_TO_DATE_ERROR(Pattern.compile("^cannot convert datum from decimal to type date")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			for(String val : sql.getVariables().values()) {
				if(!val.startsWith("'") || !val.endsWith("'")) {
					return true;
				}
			}
			return false;
		}
	},
	INCORRECT_FLOAT(Pattern.compile("^Incorrect float value: '(?<val>.*)' for column '(?<columnName>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				Float.valueOf(val);
			} catch (Exception e) {
				return true;
			}
			return false;
		}
	},
	INCORRECT_DOUBLE(Pattern.compile("^Incorrect double value: '(?<val>.*)' for column '(?<columnName>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				Float.valueOf(val);
			} catch (Exception e) {
				return true;
			}
			return false;
		}
	},
	INCORRECT_BIGINT(Pattern.compile("^Incorrect bigint value: '(?<val>.*)' for column '(?<columnName>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups,
				SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				Long.valueOf(val);
			} catch (Exception e) {
				return true;
			}
			return false;
		}
	},
	INCORRECT_INTEGER(Pattern.compile("^Incorrect int value: '(?<val>.*)' for column '(?<columnName>.*)'")) {
		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			String val = namedGroups.get("val");
			try {
				Integer.valueOf(val);
			} catch (Exception e) {
				return true;
			}
			return false;
		}
	},
	NOT_NUMBER(Pattern.compile("^Data truncated for column '(?<columnName>.*)'")) {

		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			
			String columnName = namedGroups.get("columnName");
			try {
				Double.valueOf(sql.getVariables().get(columnName));
			} catch (Exception e) {
				return true;
			}
			return false;
		}
	},
	OUT_OF_RANGE(Pattern.compile("^Data truncation: Out of range value for column '(?<columnName>.*)'")) {

		@Override
		public boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine) {
			
			String columnName = namedGroups.get("columnName");
			String value = sql.getVariables().get(columnName);
			
			boolean flag = false;
			for(Schema s : latestSchemaList) {
				String type = s.getColumns().get(columnName);
				if(type == null) {
					continue;
				}
				
				flag = isOutOfRange(value, ColumnType.findByName(type));
				if(flag == true) {
					return true;
				}
			}
			
			System.out.println("out of range sql=" + sql + " groups=" + namedGroups);
			
			return false; 
		}
	}
	;

	private Pattern rule;

	private ExceptionType(Pattern rule) {
		this.rule = rule;
	}

	public Pattern getRule() {
		return rule;
	}

	public void setRule(Pattern rule) {
		this.rule = rule;
	}
	
	public static boolean isOutOfRange(String value, ColumnType type) {
		try {
			switch (type) {
			case INT:
				Integer.valueOf(value);
				break;
			case LONG:
				Long.valueOf(value);
				break;
			case DOUBLE:
				Double.valueOf(value);
				break;
			case FLOAT:
				Float.valueOf(value);
				break;
			default:
//				System.out.println("should not be here.type="+type);
			}
			
		} catch (Exception e) {
			return true;
		}
		
		return false;
		
	}

	public abstract boolean verify(Sql sql, Collection<Schema> latestSchemaList, Map<String, String> namedGroups, SchemaLine schemaLine);
	
	private static Logger logger = LoggerFactory.getLogger(ExceptionType.class);

}
