package org.restsql.core.impl.oracle;

import org.restsql.core.impl.AbstractSqlBuilder;

public class OracleSqlBuilder extends AbstractSqlBuilder {

	public OracleSqlBuilder() {
	}

	@Override
	protected String buildSelectLimitSql(int limit, int offset) {
		return "";
	}

}
