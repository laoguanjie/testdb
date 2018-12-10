package test.db;

import lombok.Data;

@Data
public class DDLSql extends Sql {

	private DDLOperation ddlOperation;
}
