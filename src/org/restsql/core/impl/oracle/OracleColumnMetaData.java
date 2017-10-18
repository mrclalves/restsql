/* Copyright (c) restSQL Project Contributors. Licensed under MIT. */
package org.restsql.core.impl.oracle;

import java.sql.Types;

import org.restsql.core.BinaryObject;
import org.restsql.core.InvalidRequestException;
import org.restsql.core.RequestValue;
import org.restsql.core.impl.ColumnMetaDataImpl;

/**
 * Oracle specific implementation.
 * 
 * @author marcelo.alves
 */

public class OracleColumnMetaData extends ColumnMetaDataImpl {

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
					case Types.TIME:
					case Types.TIMESTAMP:
					default:
						// do nothing
				}
			} catch (final NumberFormatException e) {
				throw new InvalidRequestException("Could not convert " + requestValue.getName() + " value " + value + " to number");
			}
		}

		requestValue.setValue(value);
	}

}