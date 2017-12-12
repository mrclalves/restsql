package org.restsql.core.impl.oracle;

import org.restsql.core.Config;
import org.restsql.core.impl.AbstractDAO;

public class OracleDAOImpl extends AbstractDAO { 

	public OracleDAOImpl() {
	}

	@Override
	protected String getSqlAllResources() {
		String sql = Config.properties.getProperty(Config.KEY_DATABASE_SQL_ALL_RESOURCES,
				Config.DEFAULT_DATABASE_SQL_ALL_RESOURCES);
		return sql;
	}

	@Override
	protected String getSqlResourceByName() {
		String sql = Config.properties.getProperty(Config.KEY_DATABASE_SQL_RESOURCE_BYNAME,
				Config.DEFAULT_DATABASE_SQL_RESOURCE_BYNAME);
		return sql;
	}
}
