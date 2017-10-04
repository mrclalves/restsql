/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.service.monitoring;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.restsql.core.Config;
import org.restsql.core.Factory;

import com.codahale.metrics.health.HealthCheck;

/**
 * Pings configured database.
 * 
 * @author Mark Sawers
 */
public class DatabaseConnectionHealthCheck extends HealthCheck {
	@Override
	protected Result check() throws Exception {
		Connection connection = null;
		try {
			String sqlPing = Config.properties.getProperty(Config.KEY_DATABASE_SQL_PING, Config.DEFAULT_DATABASE_SQL_PING); 
			connection = Factory.getConnection(null);
			PreparedStatement statement = connection.prepareStatement(sqlPing);
			statement.executeQuery();
			return Result.healthy();
		} catch (SQLException exception) {
			Config.logger.error("Database health check failure", exception);
			return Result.unhealthy("Can't ping database");
		} finally {
			if (connection != null) {
				connection.close();
			}
		}
	}
}
