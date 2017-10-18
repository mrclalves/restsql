package org.restsql.core.impl.oracle;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.restsql.core.ColumnMetaData;
import org.restsql.core.Config;
import org.restsql.core.Factory;
import org.restsql.core.InvalidRequestException;
import org.restsql.core.Request;
import org.restsql.core.RequestValue;
import org.restsql.core.SqlResourceMetaData;
import org.restsql.core.TableMetaData;
import org.restsql.core.impl.AbstractSqlBuilder;

public class OracleSqlBuilder extends AbstractSqlBuilder {

	public OracleSqlBuilder() {
	}

	@Override
	protected String buildSelectLimitSql(int limit, int offset) {
		return "";
	}
	
	
	/**
	 * Builds insert SQL.
	 * 
	 * @param params insert params
	 * @return map of sql struct, per table
	 * @throws InvalidRequestException if a database access error occurs
	 */
	@Override
	protected Map<String, SqlStruct> buildInsertSql(final SqlResourceMetaData metaData, final Request request,
			final boolean doParent) throws InvalidRequestException {

		final Map<String, SqlStruct> sqls = new HashMap<String, SqlStruct>(metaData.getNumberTables());
		Map<String, Boolean> tablesHasPrimaryKeyParam = new HashMap<String, Boolean>(metaData.getNumberTables());

		// Iterate through the params and build the sql for each table
		for (final RequestValue param : request.getParameters()) {
			final List<TableMetaData> tables = metaData.getWriteTables(request.getType(), doParent);
			for (final TableMetaData table : tables) {
				final ColumnMetaData column = table.getColumns().get(param.getName());
				if (column != null) {
					if (column.isReadOnly()) {
						throw new InvalidRequestException(InvalidRequestException.MESSAGE_READONLY_PARAM,
								column.getColumnLabel());
					}
					final String qualifiedTableName = column.getQualifiedTableName();
					SqlStruct sql = sqls.get(qualifiedTableName);
					if (sql == null) {
						// Create new sql holder
						sql = new SqlStruct(DEFAULT_INSERT_SIZE, DEFAULT_INSERT_SIZE / 2);
						sqls.put(qualifiedTableName, sql);
						sql.getMain().append("INSERT INTO ");
						sql.getMain().append(qualifiedTableName);
						sql.getMain().append(" (");

						sql.appendToBothClauses(" VALUES (");
					} else {
						sql.getMain().append(',');
						sql.appendToBothClauses(",");
					}
					sql.getMain().append(column.getColumnName()); // since parameter may use column label

					// Begin quote the column value
					if (column.isCharOrDateTimeType() && param.getValue() != null) {
						sql.getClause().append('\'');
					}

					// Convert String to appropriate object
					column.normalizeValue(param);

					// Set the value in the printable clause, the ? in the prepared clause, and prepared clause value
					sql.getClause().append(param.getValue());
					sql.getPreparedClause().append(buildPreparedParameterSql(column));
					sql.getPreparedValues().add(param.getValue());

					// End quote the column value
					if (column.isCharOrDateTimeType() && param.getValue() != null) {
						sql.getClause().append('\'');
					}
					
					// identify if is qualifiedTableName was add
					if (!tablesHasPrimaryKeyParam.containsKey(qualifiedTableName)) {
						tablesHasPrimaryKeyParam.put(qualifiedTableName, new Boolean(false));
					}
					
					// identify if is column sequence and has value
					if (column.isSequence() && param.getValue() != null) {
						tablesHasPrimaryKeyParam.put(qualifiedTableName, new Boolean(true));
					}
				}
			}
		}
		
		for (final String tableName : sqls.keySet()) {
			final SqlStruct sql = sqls.get(tableName);
			if (sql == null) {
				sqls.remove(tableName);
			} else {
				final List<TableMetaData> tables = metaData.getWriteTables(request.getType(), doParent);
				for (TableMetaData tableMetaData : tables) {
					String qualifiedTableName = tableMetaData.getQualifiedTableName();
					
					if (!qualifiedTableName.equals(tableName)) continue;
					
					Boolean hasPrimaryKeyParm = tablesHasPrimaryKeyParam.get(tableName);
					if (hasPrimaryKeyParm) continue;

					Connection connection = null; 
					try {
						connection = Factory.getConnection(tableMetaData.getDatabaseName());
					} catch (Exception e) {
						Config.logger.error("Error connection database ["+tableMetaData.getDatabaseName()+"], " + e.getMessage(), e);
						throw new InvalidRequestException(e);
					} 
					
					Set<String> collectionKey = tableMetaData.getColumns().keySet();
					for (String keyColumn : collectionKey) {
						final ColumnMetaData column = tableMetaData.getColumns().get(keyColumn);
						if (column.isSequence()) {
							sql.getMain().append(',');
							sql.appendToBothClauses(",");
							sql.getMain().append(column.getColumnName()); // since parameter may use column label
							try {
								Long id = Factory.getSequenceManager().getNextValue(connection, column.getSequenceName());
								
								
								sql.getClause().append(id);
								sql.getPreparedClause().append(buildPreparedParameterSql(column));
								sql.getPreparedValues().add(id);
								
								final RequestValue param = new RequestValue(column.getColumnLabel(), id);
								request.getParameters().add(param);
							} catch (Exception e) {
								Config.logger.error("Error get next value sequence ["+column.getSequenceName()+"], " + e.getMessage(), e);
								throw new InvalidRequestException(e);
							}

						}
					}
					
				}
				
				
				sql.getMain().append(')');
				sql.appendToBothClauses(")");
				sql.compileStatements();
			}
		}

		if (sqls.size() == 0) {
			throw new InvalidRequestException(InvalidRequestException.MESSAGE_INVALID_PARAMS);
		}
		return sqls;
	}

}
