package org.restsql.core.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.restsql.core.Config;
import org.restsql.core.SqlResourceException;

public abstract class AbstractDAO {

	private static final Log logger = LogFactory.getLog(Config.NAME_LOGGER_ACCESS);

	public AbstractDAO() {
		super();
	}

	public List<ResourceBean> getAllResources(Connection connection) throws SqlResourceException {
		PreparedStatement ps = null;
		ResultSet resultSet = null;
		try {
			List<ResourceBean> list = new ArrayList<ResourceBean>();
			String sql = getSqlAllResources();
			ps = connection.prepareStatement(sql);
			resultSet = ps.executeQuery();
			while (resultSet.next()) {
				ResourceBean rb = createResource(resultSet);
				list.add(rb);
			}
			return list;
		} catch (Exception e) {
			logger.error("### Error " + e.getMessage(), e);
			throw new SqlResourceException(e, " error find resources XML in data base.");
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
			}
		}
	}

	protected abstract String getSqlAllResources();

	protected ResourceBean createResource(ResultSet resultSet) throws SQLException, SqlResourceException {
		ResourceBean rb = new ResourceBean();
		rb.setId(resultSet.getInt(1));
		rb.setName(resultSet.getString(2));
		Clob clob = resultSet.getClob(3);
		rb.setResourceXML(clobToString(clob));
		return rb;
	}

	public ResourceBean getResourceByName(Connection connection, String resName) throws SqlResourceException {
		PreparedStatement ps = null;
		ResultSet resultSet = null;
		try {
			String sql = getSqlResourceByName();
			ps = connection.prepareStatement(sql);
			ps.setString(1, resName);
			resultSet = ps.executeQuery();
			ResourceBean rb = null;
			if (resultSet.next()) {
				rb = createResource(resultSet);
			}
			return rb;
		} catch (Exception e) {
			logger.error("### Error " + e.getMessage(), e);
			throw new SqlResourceException(e, " error find resources XML in data base.");
		} finally {
			try {
				if (resultSet != null)
					resultSet.close();
				if (ps != null)
					ps.close();
			} catch (SQLException e) {
			}
		}
	}

	protected abstract String getSqlResourceByName();

	protected String clobToString(Clob clob) throws SqlResourceException {
		String returnString = "";
		char[] array;

		try {
			if (clob != null) {
				BufferedReader reader = new BufferedReader(clob.getCharacterStream());
				Long i = clob.length();

				array = new char[i.intValue()];
				reader.read(array, 0, i.intValue());
				returnString = new String(array);
			}

		} catch (IOException e) {
			logger.error("Error of IO in parse Clob to String, " + e.getMessage(), e);
			throw new SqlResourceException(e, "### Error of IO in parse Clob to String, " + e.getMessage());
		} catch (SQLException e) {
			logger.error("Error of SQL in parse Clob to String, " + e.getMessage(), e);
			throw new SqlResourceException(e, "### Error of SQL in parse Clob to String, " + e.getMessage());
		}
		return returnString;
	}

}