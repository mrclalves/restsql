package org.restsql.core.impl.serial;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ConvertFromat {

	private static Pattern patterCamelCaseToSnakeCase = Pattern.compile("(?<=[a-z])[A-Z]");

	public ConvertFromat() {
	}

	public static String camelCaseToSnakeCase(String text) {

		Matcher m = patterCamelCaseToSnakeCase.matcher(text);

		StringBuffer sb = new StringBuffer();
		while (m.find()) {
			m.appendReplacement(sb, "_" + m.group().toLowerCase());
		}
		m.appendTail(sb);

		return sb.toString();
	}

	public static String snakeCaseToCamelCase(String text) {

		StringBuilder sb = new StringBuilder(text.toLowerCase());
		for (int i = 0; i < sb.length(); i++) {
			if (sb.charAt(i) == '_') {
				sb.deleteCharAt(i);
				sb.replace(i, i + 1, String.valueOf(Character.toUpperCase(sb.charAt(i))));
			}
		}

		return sb.toString();
	}

}
