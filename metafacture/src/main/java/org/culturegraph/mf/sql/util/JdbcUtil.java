/*
 *  Copyright 2013 Christoph Böhme
 *
 *  Licensed under the Apache License, Version 2.0 the "License";
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.culturegraph.mf.sql.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.EnumSet;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.culturegraph.mf.exceptions.MetafactureException;

/**
 * Utility functions for working with JDBC.
 *
 * @author Christoph Böhme
 *
 */
public final class JdbcUtil {

	private static final String[] TYPES = { "TABLE", "VIEW" };

	public enum Bug {
		/**
		 * The Postgresql JDBC driver simply tacks a RETURNING-clause to the sql
		 * statement without checking if the statement supports this clause.
		 */
		RETURN_GENERATED_KEYS_PRODUCES_INVALID_SQL,

		/**
		 * The Sqlite JDBC driver does not return null if the current result is
		 * not a result set but throws an exception instead.
		 */
		GET_RESULT_SET_THROWS_ILLEGAL_EXCEPTION,

		/**
		 * The Sqlite JDBC driver does not return -1 if the current result is
		 * not an update count but throws an exception instead.
		 */
		GET_UPDATE_COUNT_THROWS_ILLEGAL_EXCEPTION,

		/**
		 * The MySQL Connector/J only streams result sets if statements are
		 * configured with a fetch size of {@code Integer.MIN_VALUE} and the
		 * options TYPE_FORWARD_ONLY and CONCUR_READ_ONLY. See
		 * https://dev.mysql.
		 * com/doc/connector-j/en/connector-j-reference-implementation
		 * -notes.html for details. Since this fetch size setting does not seem
		 * to be specified JDBC behaviour, we handle it as a special case.
		 */
		RESULT_SET_STREAMING_ONLY_IF_FETCH_SIZE_IS_MIN_VALUE
	}

	public static EnumSet<Bug> getDriverBugs(final Connection connection) {
		final EnumSet<Bug> driverBugs = EnumSet.noneOf(Bug.class);

		final String driverName;
		try {
			driverName = connection.getMetaData().getDriverName();
		} catch (final SQLException e) {
			throw new MetafactureException(e);
		}

		if ("PostgreSQL Native Driver".equals(driverName)) {
			driverBugs.add(Bug.RETURN_GENERATED_KEYS_PRODUCES_INVALID_SQL);
		} else if ("SQLiteJDBC".equals(driverName)) {
			driverBugs.add(Bug.GET_RESULT_SET_THROWS_ILLEGAL_EXCEPTION);
			driverBugs.add(Bug.GET_UPDATE_COUNT_THROWS_ILLEGAL_EXCEPTION);
		} else if ("MySQL-AB JDBC Driver".equals(driverName)
				|| "MySQL Connector Java".equals(driverName)) {
			driverBugs
					.add(Bug.RESULT_SET_STREAMING_ONLY_IF_FETCH_SIZE_IS_MIN_VALUE);
		}
		// TODO append the oracle dirver
		return driverBugs;
	}

	public static Connection getConnection(final String datasourceName) {
		try {
			final InitialContext ctx = new InitialContext();
			final DataSource datasource = (DataSource) ctx
					.lookup(datasourceName);
			return datasource.getConnection();
		} catch (final NamingException ne) {
			throw new MetafactureException(ne);
		} catch (final SQLException se) {
			throw new MetafactureException(se);
		}
	}

	public static void closeConnection(final Connection connection) {
		try {
			DbUtils.commitAndClose(connection);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static ResultSet getTableMetadata(Connection jdbcConnection,
			String tableNamePattern, String schema, String catalog,
			boolean isQuoted) {
		ResultSet rs = null;
		try {
			DatabaseMetaData meta = jdbcConnection.getMetaData();
			if ((isQuoted && meta.storesMixedCaseQuotedIdentifiers())) {
				rs = meta.getTables(catalog, schema, tableNamePattern, TYPES);
			} else if ((isQuoted && meta.storesUpperCaseQuotedIdentifiers())
					|| (!isQuoted && meta.storesUpperCaseIdentifiers())) {
				rs = meta.getTables(StringHelper.toUpperCase(catalog),
						StringHelper.toUpperCase(schema),
						StringHelper.toUpperCase(tableNamePattern), TYPES);
			} else if ((isQuoted && meta.storesLowerCaseQuotedIdentifiers())
					|| (!isQuoted && meta.storesLowerCaseIdentifiers())) {
				rs = meta.getTables(StringHelper.toLowerCase(catalog),
						StringHelper.toLowerCase(schema),
						StringHelper.toLowerCase(tableNamePattern), TYPES);
			} else {
				rs = meta.getTables(catalog, schema, tableNamePattern, TYPES);
			}
			return rs;
			// while (rs.next()) {
			// String tableName = rs.getString("TABLE_NAME");
			// System.out.println("table = " + tableName);
			// }

		} catch (SQLException sqlException) {
			sqlException.printStackTrace();
		} finally {
			try {
				DbUtils.close(rs);
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	static class StringHelper {
		public static String toUpperCase(String str) {
			if (isNull(str))
				throw new NullPointerException();
			return str.toUpperCase();
		}

		public static String toLowerCase(String str) {
			if (isNull(str))
				throw new NullPointerException();
			return str.toLowerCase();
		}

		public static boolean isNull(String str) {
			return str == null;
		}

	}

}
