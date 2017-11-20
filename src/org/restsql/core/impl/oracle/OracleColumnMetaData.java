/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl.oracle;

import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.restsql.core.BinaryObject;
import org.restsql.core.Config;
import org.restsql.core.InvalidRequestException;
import org.restsql.core.RequestValue;
import org.restsql.core.impl.ColumnMetaDataImpl;

/**
 * Oracle specific implementation.
 * 
 * @author marcelo.alves
 */

public class OracleColumnMetaData extends ColumnMetaDataImpl {
	
	private SimpleDateFormat sdfTimestampo = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	private SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
	
	
	@Override
	public Object getResultByLabel(final ResultSet resultSet) throws SQLException {
		if (isBinaryType()) {
			return new BinaryObject(resultSet.getBytes(getQualifiedColumnLabel()));
		} else {
			return normalizeReturnValueByNumber(resultSet);
		}
	}

	@Override
	public Object getResultByNumber(final ResultSet resultSet) throws SQLException {
		if (isBinaryType()) {
			return new BinaryObject(resultSet.getBytes(getColumnNumber()));
		} else {
			return normalizeReturnValueByNumber(resultSet);
		}
	}
	
	@Override
	@SuppressWarnings("fallthrough")
	public void normalizeValue(final RequestValue requestValue) throws InvalidRequestException {
		Object value = requestValue.getValue();
		if (value instanceof String && requestValue.getOperator() != RequestValue.Operator.In) {
			try {
				switch (getColumnType()) {
					case Types.BOOLEAN:
						value = Boolean.valueOf((String) value);
						break;

					case Types.BIT:
					case Types.TINYINT:
					case Types.SMALLINT:
					case Types.INTEGER:
						value = Integer.valueOf((String) value);
						break;

					case Types.NUMERIC:
					case Types.BIGINT:
						value = Long.valueOf((String) value);
						break;

					case Types.DECIMAL:
					case Types.FLOAT:
					case Types.REAL:
						value = Float.valueOf((String) value);
						break;

					case Types.DOUBLE:
						value = Double.valueOf((String) value);
						break;

					case Types.BINARY:
					case Types.BLOB:
					case Types.JAVA_OBJECT:
					case Types.LONGVARBINARY:
						if (BinaryObject.isStringBase64((String) value)) {
							value = BinaryObject.fromString((String) value);
						} else {
							throw new InvalidRequestException(
									InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE,
									requestValue.getName());
						}

					case Types.DATE:
						try {
							Date date = sdfDate.parse(value.toString());
							value = new java.sql.Date(date.getTime());
							
						} catch (ParseException e) {
							Config.logger.error("Erro parse date, " + value.getClass(), e);
							value = null;
						}
						break;
					case Types.TIME:
					case Types.TIMESTAMP:
						try {
							Date date = sdfTimestampo.parse(value.toString());
							Timestamp timestamp = new Timestamp(date.getTime());
							value = timestamp;
							
						} catch (ParseException e) {
							Config.logger.error("Erro parse date, " + value.getClass(), e);
							value = null;
						}
						break;
					default:
				}
			} catch (final NumberFormatException e) {
				throw new InvalidRequestException("Could not convert " + requestValue.getName() + " value " + value + " to number");
			}
		}

		requestValue.setValue(value);
	}
	
	public Object normalizeReturnValueByNumber(final ResultSet resultSet) throws SQLException {
		Object value = null;
		try {
			switch (getColumnType()) {
				case Types.BOOLEAN:
					value = resultSet.getBoolean(getColumnNumber());
					break;
					
				case Types.BIT:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
					value = Integer.valueOf(resultSet.getInt(getColumnNumber()));
					break;
					
				case Types.NUMERIC:
				case Types.BIGINT:
					value = Long.valueOf(resultSet.getLong(getColumnNumber()));
					break;
					
				case Types.DECIMAL:
				case Types.FLOAT:
				case Types.REAL:
					value = Float.valueOf(resultSet.getFloat(getColumnNumber()));
					break;
					
				case Types.DOUBLE:
					value = Double.valueOf(resultSet.getDouble(getColumnNumber()));
					break;
					
				case Types.BINARY:
				case Types.BLOB:
				case Types.JAVA_OBJECT:
				case Types.LONGVARBINARY:
					Blob blob = resultSet.getBlob(getColumnNumber());
					if (blob != null) {
						if (BinaryObject.isStringBase64(blob.toString())) {
							value = BinaryObject.fromString(blob.toString());
						} else {
							throw new SQLException(String.format(
									InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE, getQualifiedColumnLabel()));
						}
					}
					break;
					
				case Types.CLOB:
					try {
						Clob clob = resultSet.getClob(getColumnNumber());
						if (clob != null) {
							StringBuffer sb = new StringBuffer((int) clob.length());
							Reader r = clob.getCharacterStream();
							char[] cbuf = new char[2048];
							int n = 0;
							while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
								if (n > 0) {
									sb.append(cbuf, 0, n);
								}
							}
							value = sb.toString();
						}

					} catch (Exception e) {
						Config.logger.error(String.format(InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE,
								getQualifiedColumnLabel()), e);
						throw new SQLException(
								String.format(InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE,
										getQualifiedColumnLabel()));
					}
					break;
					
				case Types.DATE:
					try {
						java.sql.Date dateValue = resultSet.getDate(getColumnNumber()); 
						if (dateValue != null)
							value = sdfDate.format(dateValue);
					} catch (Exception e) {
						Config.logger.error("Erro parse date, " + value.getClass(), e);
						value = null;
					}
					break;
					
				case Types.TIME:
				case Types.TIMESTAMP:
					try {
						Timestamp timestamp = resultSet.getTimestamp(getColumnNumber()); 
						if (timestamp != null)
							value = sdfTimestampo.format(timestamp);
					} catch (Exception e) {
						Config.logger.error("Erro parse date, " + value.getClass(), e);
						value = null;
					}
					break;
					
				default:
					value = resultSet.getString(getColumnNumber());
			}
		} catch (final Exception e) {
			throw new SQLException("Could not convert " + getQualifiedColumnLabel(), e);
		}
		Config.logger.debug("###REST-SQL: "+getClass().getName()+" normalizeReturnValueByNumber ["+getColumnTypeName()+"]");
		try {
			Config.logger.debug("###REST-SQL: "+getClass().getName()+" ["+value.toString()+"]");
		} catch (Exception e) {
		}
		
		return value;
	}
	
	public Object normalizeReturnValueByLabel(final ResultSet resultSet) throws SQLException {
		Object value = null;
		try {
			switch (getColumnType()) {
				case Types.BOOLEAN:
					value = resultSet.getBoolean(getQualifiedColumnLabel());
					break;
					
				case Types.BIT:
				case Types.TINYINT:
				case Types.SMALLINT:
				case Types.INTEGER:
					value = Integer.valueOf(resultSet.getInt(getQualifiedColumnLabel()));
					break;
					
				case Types.NUMERIC:
				case Types.BIGINT:
					value = Long.valueOf(resultSet.getLong(getQualifiedColumnLabel()));
					break;
					
				case Types.DECIMAL:
				case Types.FLOAT:
				case Types.REAL:
					value = Float.valueOf(resultSet.getFloat(getQualifiedColumnLabel()));
					break;
					
				case Types.DOUBLE:
					value = Double.valueOf(resultSet.getDouble(getQualifiedColumnLabel()));
					break;
					
				case Types.BINARY:
				case Types.BLOB:
				case Types.JAVA_OBJECT:
				case Types.LONGVARBINARY:
					Blob blob = resultSet.getBlob(getQualifiedColumnLabel());
					if (BinaryObject.isStringBase64(blob.toString())) {
						value = BinaryObject.fromString(blob.toString());
					} else {
						throw new SQLException(String.format(
								   InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE, getQualifiedColumnLabel()));
					}
					break;
					
				case Types.CLOB:
					try {
						Clob clob = resultSet.getClob(getQualifiedColumnLabel());
						StringBuffer sb = new StringBuffer((int) clob.length());
						Reader r = clob.getCharacterStream();
						char[] cbuf = new char[2048];
						int n = 0;
						while ((n = r.read(cbuf, 0, cbuf.length)) != -1) {
							if (n > 0) {
								sb.append(cbuf, 0, n);
							}
						}
						value = sb.toString();

					} catch (Exception e) {
						Config.logger.error(String.format(InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE,
								getQualifiedColumnLabel()), e);
						throw new SQLException(
								String.format(InvalidRequestException.MESSAGE_CANNOT_BASE64DECODE,
										getQualifiedColumnLabel()));
					}
					break;
					
				case Types.DATE:
					value = resultSet.getDate(getQualifiedColumnLabel()); 
					break;
					
				case Types.TIME:
				case Types.TIMESTAMP:
					value = resultSet.getTimestamp(getQualifiedColumnLabel());
					break;
					
				default:
					value = resultSet.getString(getQualifiedColumnLabel());
			}
		} catch (final Exception e) {
			throw new SQLException("Could not convert " + getQualifiedColumnLabel(), e);
		}

		Config.logger.debug("###REST-SQL: "+getClass().getName()+" normalizeReturnValueByLabel ["+getColumnTypeName()+"]");
		try {
			Config.logger.debug("###REST-SQL: "+getClass().getName()+" ["+value.toString()+"]");
		} catch (Exception e) {
		}
		return value;
	}

}