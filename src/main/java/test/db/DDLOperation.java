package test.db;

public enum DDLOperation {

	ADD_COLUMN("alter table {table} add column {key} {type}") {
		@Override
		public String sql(String tableName, String key, String type) {
			return super.sql(tableName, key, type);
		}
	},
	MODIFY_COLUMN("alter table {table} modify column {key} {type}") {
		@Override
		public String sql(String tableName, String key, String type) {
			return super.sql(tableName, key, type);
		}
	},
	DROP_COLUMN("alter table {table} drop column {key}") {
		@Override
		public String sql(String tableName, String key, String type) {
			return super.sql(tableName, key, type);
		}
	},
	ADD_INDEX("alter table {table} add index {key}_id({key})") {
		@Override
		public String sql(String tableName, String key, String type) {
			return super.sql(tableName, key, type);
		}
	},
	DROP_INDEX("alter table {table} drop index {key}_id") {
		@Override
		public String sql(String tableName, String key, String type) {
			return super.sql(tableName, key, type);
		}
	};

	private String template;

	private DDLOperation(String template) {
		this.template = template;
	}

	public String sql(String tableName, String key, String type) {
		return template.replace("{table}", tableName).replace("{key}", key).replace("{type}", type);
	}
	
}
