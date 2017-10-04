package org.restsql.core.impl;

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.restsql.core.Config;
import org.restsql.core.Factory.ConnectionFactory;

/**
 * Simple pooled connection factory that use connection pool. The caller must close the
 * connection but it is ony released to pool. The factory uses the database property pool - poll name.
 * 
 * @author Piotr Roznicki
 * @author modify by marcelo.alves
 */
public class ConnectionPoolFactoryImpl implements ConnectionFactory {
	private final String connectionName;
	private final String lookupName;
	
	private Context envCtxA;
    private DataSource dataSource;

	public ConnectionPoolFactoryImpl() {
		this.connectionName = Config.properties.getProperty("database.pool","DEFAULT");
		this.lookupName = Config.properties.getProperty("database.lookup","java:comp/env");
	}

	public Connection getConnection(String defaultDatabase) throws SQLException {
		java.sql.Connection connection = null;
		try {
			connection = getDataSource().getConnection();
		} catch (Exception e) {
			throw (new SQLException("Error getting connection from pool[" + this.connectionName + "] lookup ["+this.lookupName+"], " + e.getMessage()));
		}
		if (defaultDatabase != null) {
			connection.setCatalog(defaultDatabase);
		}
		return connection;
	}

	protected DataSource getDataSource() throws NamingException {
		if (this.dataSource == null) {
			this.dataSource = (DataSource) getEnvCtxA().lookup(this.connectionName);
		}
		return this.dataSource;
	}
	
	public Context getEnvCtxA() throws NamingException {
		if (this.envCtxA == null) {
			InitialContext initialContext = new InitialContext();
			Context envCtxA = (Context) initialContext.lookup(this.lookupName);
			this.envCtxA = envCtxA;
		}
		return this.envCtxA;
	}

	public void destroy() throws SQLException {
	}
}
