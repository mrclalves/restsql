package org.restsql.tools.impl.oracle;

import org.restsql.tools.impl.AbstractResourceDefinitionGenerator;

public class OracleResourceDefinitionGenerator extends AbstractResourceDefinitionGenerator {

	private static final String SQL_COLUMNS_QUERY = "SELECT A.COLUMN_NAME, A.TABLE_NAME FROM ALL_TAB_COLUMNS A INNER JOIN ALL_OBJECTS B ON A.OWNER = B.OWNER AND A.TABLE_NAME = B.OBJECT_NAME AND B.OBJECT_TYPE = 'TABLE' WHERE A.OWNER = ? ";
	
	public OracleResourceDefinitionGenerator() {
	}

	@Override
	public String getColumnsQuery() {
		return SQL_COLUMNS_QUERY;
	}
}
