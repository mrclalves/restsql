/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.restsql.core.Config;
import org.restsql.core.SequenceManager;
import org.restsql.core.SqlResourceException;

/**
 * @author Mark Sawers
 */
public abstract class AbstractSequenceManager implements SequenceManager {

	@Override
	public int getCurrentValue(final Connection connection, String sequenceName) throws SqlResourceException {
		final String sql = getCurrentValueSql(sequenceName);
		Statement statement = null;
		try {
			statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			resultSet.next();
			return resultSet.getInt(1);
		} catch (final SQLException exception) {
			throw new SqlResourceException(exception, sql);
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}
	}
	
	public abstract String getCurrentValueSql(String sequenceName);
	
	protected long execSQLSequence(final Connection connection, final String sqlSequence) throws SqlResourceException {
		
		Long id = null;
		Statement statement = null;
		try {
			statement = connection.createStatement();
			if (Config.logger.isDebugEnabled()) {
				Config.logger.debug("\t[setUp] " + sqlSequence);
			}
			ResultSet resultSet = statement.executeQuery(sqlSequence);
			
			if(resultSet.next()) {
				id = resultSet.getLong(1);	
			}
		} catch (final SQLException exception) {
			throw new SqlResourceException(exception, sqlSequence);
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (SQLException e) {
				}
			}
		}
		return id;
	}
	
	/** Retrieves next sequence value. */
	public long getNextValue(final Connection connection, String sequenceName) throws SqlResourceException {
		return execSQLSequence(connection, getNextValueSql(sequenceName));
	}

	protected String getNextValueSql(String sequenceName) {
		return null;
	}
}
