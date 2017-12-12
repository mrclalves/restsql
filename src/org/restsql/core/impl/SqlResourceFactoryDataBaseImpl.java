package org.restsql.core.impl;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.restsql.core.Config;
import org.restsql.core.Factory;
import org.restsql.core.Factory.SqlResourceFactory;
import org.restsql.core.Factory.SqlResourceFactoryException;

public class SqlResourceFactoryDataBaseImpl extends SqlResourceFactoryImpl implements SqlResourceFactory {

	private String database;
	private AbstractDAO abstractDAO;

	public SqlResourceFactoryDataBaseImpl() {
	}

	@Override
	public List<String> getSqlResourceNames() throws SqlResourceFactoryException {
		List<ResourceBean> allResources;
		Connection connection = null;
		try {
			connection = getConnection();
			allResources = getAbstractDAO().getAllResources(connection);
			List<String> listResourcesName = new ArrayList<String>();
			for (ResourceBean resourceBean : allResources) {
				listResourcesName.add(resourceBean.getName());
			}
			return listResourcesName;
		} catch (Exception e) {
			throw new SqlResourceFactoryException("Resources didn't find in data base.");
		} finally {
			if (connection != null) {
				try {
					connection.close();
				} catch (Exception e) {
				}
			}
		}
	}

	protected Connection getConnection() throws SQLException {
		return Factory.getConnection(getDatabase());
	}


	/**
	 * @return the database
	 */
	private String getDatabase() {
		if (this.database == null) {
			this.database = Config.properties.getProperty(Config.KEY_DATABASE_RESOURCES,
					Config.DEFAULT_DATABASE_RESOURCES);
		}
		return this.database;
	}

	/**
	 * @return the abstractDAO
	 */
	private AbstractDAO getAbstractDAO() {
		if (this.abstractDAO == null) {
			this.abstractDAO = Factory.getAbstractDAO();
		}
		return this.abstractDAO;
	}

	@Override
	protected InputStream getInputStream(String resName) throws SqlResourceFactoryException {
		Connection connection = null;
		try {
			connection = getConnection();
			ResourceBean resourceBean = getAbstractDAO().getResourceByName(connection, resName);
			ByteArrayInputStream bais = new ByteArrayInputStream(resourceBean.getResourceXML().getBytes("UTF-8"));
			return bais;
		} catch (Exception e) {
			throw new SqlResourceFactoryException("Didn't resource by name " + resName + ", " + e.getMessage());
		} finally {
			if (connection != null ) 
				try {
					connection.close();
				} catch (Exception e2) {
				}
		}
	}
}
