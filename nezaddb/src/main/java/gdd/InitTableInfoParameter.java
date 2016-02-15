package gdd;

import java.sql.DatabaseMetaData;

public class InitTableInfoParameter {
	public DatabaseMetaData database;

	public InitTableInfoParameter(DatabaseMetaData database) {
		this.database = database;
	}
}