package org.restsql.core.impl;

import java.sql.Connection;
import java.sql.SQLException;

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
	private final String lookupName;
	
    private DataSource dataSource;

	public ConnectionPoolFactoryImpl() {
		this.lookupName = Config.properties.getProperty(Config.KEY_DATABASE_DATASOURCE,Config.DEFAULT_DATABASE_DATASOURCE);
	}

	public Connection getConnection(String defaultDatabase) throws SQLException {
		java.sql.Connection connection = null;
		try {
			connection = getDataSource().getConnection();
			connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (Exception e) {
			Config.logger.error("Error getting connection from pool/lookup ["+this.lookupName+"], " + e.getMessage(), e);
			throw (new SQLException("Error getting connection from pool/lookup ["+this.lookupName+"], " + e.getMessage()));
		}
		if (defaultDatabase != null) {
			connection.setCatalog(defaultDatabase);
		}
		return connection;
	}

	protected DataSource getDataSource() throws NamingException {
//		if (this.dataSource == null) {
//			InitialContext initialContext = new InitialContext();
//			this.dataSource = (DataSource) initialContext.lookup(this.lookupName);
//		}
//		return this.dataSource;
		InitialContext initialContext = new InitialContext();

		return  (DataSource) initialContext.lookup(this.lookupName);
	}

	public void destroy() throws SQLException {
	}
}
