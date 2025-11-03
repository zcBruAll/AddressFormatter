package dev.allanbrunner.addressFormatter.address;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import dev.allanbrunner.addressFormatter.db.SqlClient;
import dev.allanbrunner.addressFormatter.db.TableSql;
import dev.allanbrunner.addressFormatter.db.table.AddressLinkTable;
import dev.allanbrunner.addressFormatter.db.table.AddressRawTable;
import dev.allanbrunner.addressFormatter.db.table.AddressRefTable;
import dev.allanbrunner.addressFormatter.db.table.Column;

public final class AddressFormatterService {
	private static final Pattern TITLE_RE = Pattern.compile("(?i)^\\s*(FRAU|HERR|MADAME|MONSIEUR|MR|MS|M|MME)\\s*$");
	private static final Pattern ZIP_RE = Pattern.compile("^(\\d{4})(?:[-\\s]?(\\d{2}))?$");
	private static final Pattern HOUSE_RE = Pattern.compile("(?ix)^\\s*(?<street>.+?)\\s*"
			+ "(?<number>[1-9]\\d{0,3}(?:\\s*(?:(?:bis|ter|quater|quinquies)|[A-Za-z]))?(?:/[1-9]\\d{0,3})?)\\s*$");
	private static final Pattern STREET_ONLY_RE = Pattern.compile("(?iu)^\\s*(?<street>" +
	// FR prefixes
			"(?:rue|route|rte|chemin|chem|ch|avenue|av|boulevard|bd|quai|place|pl|allée|allee|impasse|passage|promenade|sentier)\\.?\\s+[\\p{L}'’.-]+(?:\\s+[\\p{L}'’.-]+)*"
			+ "|" +
			// DE suffixes
			"[\\p{L}'’.-]+(?:\\s+[\\p{L}'’.-]+)*\\s+(?:strasse|str\\.?|gasse|weg|platz|allee|ufer|ring|quai|promenade)"
			+ "|" +
			// IT prefixes
			"(?:via|viale|vicolo|piazza|largo|salita|corso)\\.?\\s+[\\p{L}'’.-]+(?:\\s+[\\p{L}'’.-]+)*" + ")\\s*$");
	private static final Pattern POSTAL_BOX_RE = Pattern
			.compile("(?ix)^\\s*(?:P\\.O\\.\\s*Box|Postfach|Case\\s+Postale|Casella\\s+Postale|CP)\\s+(\\d{1,4})\\s*$");

	private AddressFormatterService() {}

	public static List<UnstructuredAddress> getUnstructuredAddresses(String table, String colId, String[] colLines,
			SqlClient client) throws SQLException {
		List<String> selectItems = new ArrayList<>(7);
		selectItems.add(colId);
		for (int idx = 0; idx < 6; idx++) {
			if (idx < colLines.length && colLines[idx] != null) {
				selectItems.add(colLines[idx]);
			} else {
				selectItems.add("'' AS line" + (idx + 1));
			}
		}

		String query = "SELECT " + String.join(", ", selectItems) + " FROM " + table;
		List<List<String>> rows = client.executeQuery(query, false);

		List<UnstructuredAddress> result = new ArrayList<>(rows.size());
		for (List<String> row : rows) {
			String id = Objects.requireNonNull(row.get(0), "No ID retrieved").trim();
			String[] lines = new String[6];
			for (int i = 0; i < 6; i++) {
				lines[i] = clean(row.get(i + 1));
			}
			result.add(new UnstructuredAddress(id, lines));
		}
		return result;
	}

	public static List<UnstructuredAddress> getUnstructuredAddresses(AddressRawTable rawTable, SqlClient client)
			throws SQLException {
		List<List<String>> rows = client.executeQuery(TableSql.selectTable(rawTable), false);
		List<UnstructuredAddress> result = new ArrayList<>(rows.size());
		for (List<String> row : rows) {
			String id = Objects.requireNonNull(row.get(0), "No ID retrieved").trim();
			boolean hasIban = rawTable.findColumn("iban") != null;
			boolean hasAccountOwner = rawTable.findColumn("accountOwner") != null;
			String iban = hasIban ? row.get(1) : "";
			String accountOwner = hasAccountOwner ? row.get(2) : "";
			String[] lines = new String[6];
			Arrays.fill(lines, "");
			Integer dx = 0 + (hasIban ? 1 : 0) + (hasAccountOwner ? 1 : 0);
			for (int i = 0; i < row.size() - 1 - dx; i++) {
				lines[i] = clean(row.get(i + 1 + dx));
			}

			result.add(new UnstructuredAddress(id, lines, iban, accountOwner));
		}
		return result;
	}

	public static StructuredAddress format(UnstructuredAddress raw) {
		List<String> lines = raw.lines().stream().filter(Objects::nonNull).map(String::trim).filter(s -> !s.isEmpty())
				.collect(Collectors.toList());

		int lineOffset = 0;
		String title = null;

		if (!lines.isEmpty() && TITLE_RE.matcher(lines.get(0)).matches()) {
			title = lines.get(0);
			lineOffset += 1;
		}

		String fullname = lines.size() > lineOffset ? lines.get(lineOffset) : "";
		String[] nameParts = fullname.split("\\s+");
		String firstToken = nameParts.length > 0 ? nameParts[0] : "";

		String lastname;
		String firstname = "";
		if (TITLE_RE.matcher(firstToken).matches()) {
			if (title == null || title.isBlank()) {
				title = firstToken;
			}
			lastname = nameParts.length > 1 ? nameParts[1] : "";
			firstname = nameParts.length > 2 ? nameParts[2] : "";
		} else {
			lastname = firstToken;
			firstname = nameParts.length > 1 ? nameParts[1] : "";
		}

		String compl1 = null;
		String compl2 = null;
		AddressLine address = AddressLine.street("", "");
		PostalCode postal = new PostalCode(0, null);
		String city = "";
		String streetOrPoBox = null;

		int idx = 1 + lineOffset;
		while (idx < lines.size()) {
			String line = lines.get(idx);
			idx += 1;
			String trimmed = line.trim();
			String[] localityParts = trimmed.split("\\s+", 2);
			String localityFirst = localityParts.length > 0 ? localityParts[0] : "";

			if (city.isEmpty()) {
				Matcher zipMatcher = ZIP_RE.matcher(localityFirst);
				if (zipMatcher.matches()) {
					int code = Integer.parseInt(zipMatcher.group(1));
					Integer suffix = zipMatcher.group(2) != null ? Integer.parseInt(zipMatcher.group(2)) : null;
					postal = new PostalCode(code, suffix);
					city = localityParts.length > 1 ? localityParts[1].trim() : "";
					continue;
				}
			}

			if (streetOrPoBox == null) {
				Matcher poBoxMatcher = POSTAL_BOX_RE.matcher(trimmed);
				if (poBoxMatcher.matches()) {
					streetOrPoBox = trimmed;
					address = AddressLine.poBox(trimmed);
					continue;
				}

				Matcher houseMatcher = HOUSE_RE.matcher(trimmed);
				if (houseMatcher.matches()) {
					String street = houseMatcher.group("street").trim();
					String houseNumber = houseMatcher.group("number").trim();
					streetOrPoBox = street;
					address = AddressLine.street(street, houseNumber);
					continue;
				} else {
					Matcher onlyStreetMatcher = STREET_ONLY_RE.matcher(trimmed);
					if (onlyStreetMatcher.matches()) {
						String street = trimmed;
						streetOrPoBox = street;
						address = AddressLine.street(street, "");
						continue;
					}
				}
			}

			if (compl1 == null) {
				compl1 = trimmed;
				lineOffset += 1;
			} else if (compl2 == null) {
				compl2 = trimmed;
				lineOffset += 1;
			}
		}

		String country;
		int countryIndex = 3 + lineOffset;
		if (countryIndex < lines.size()) {
			country = lines.get(countryIndex).trim();
		} else {
			country = "CH";
		}
		if (country.isEmpty()) {
			country = "CH";
		}

		String titleValue = normalizeEmpty(title);
		String lastnameValue = normalizeEmpty(lastname);
		String firstnameValue = normalizeEmpty(firstname);
		String nameValue = normalizeEmpty((lastname + " " + firstname).trim());
		String compl1Value = normalizeEmpty(compl1);
		String compl2Value = normalizeEmpty(compl2);
		String cityValue = city == null ? "" : city;

		return new StructuredAddress(raw.id(), titleValue, nameValue, lastnameValue, firstnameValue, compl1Value,
				compl2Value, address, postal, cityValue, country, raw.iban(), raw.accountOwner());
	}

	public static long updateAddrPayId(AddressLinkTable linkTable, AddressRefTable refTable, String refMainId,
			SqlClient client) throws SQLException {
		Column colOldId = refTable.findColumn("oldId");
		Column colIdOriginal = linkTable.findColumn("idOriginal");
		String sql = ("""
				UPDATE %s
				SET %s = (
					SELECT t.%s
					FROM %s t
					WHERE t.%s = %s
				)
				WHERE %s = %s
				""").formatted(linkTable.name(), linkTable.findColumn("idReferenced").dbName(),
				refTable.findColumn("id").dbName(), refTable.name(), refTable.findColumn("oldId").dbName(),
				colOldId.type().renderAsLiteral() ? SqlClient.nullableQuoted(refMainId)
						: SqlClient.nullableRaw(refMainId),
				colIdOriginal.dbName(), colIdOriginal.type().renderAsLiteral() ? SqlClient.nullableQuoted(refMainId)
						: SqlClient.nullableRaw(refMainId));

		// System.out.println("NEW UPDATE QUERY:\n" + sql);
		return client.executeNonQuery(sql);
	}

	private static String clean(String value) {
		if (value == null) {
			return "";
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? "" : trimmed;
	}

	private static String normalizeEmpty(String value) {
		if (value == null) {
			return null;
		}
		String trimmed = value.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}
}