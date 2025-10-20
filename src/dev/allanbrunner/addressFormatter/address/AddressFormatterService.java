package dev.allanbrunner.addressFormatter.address;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import dev.allanbrunner.addressFormatter.db.SqlClient;

public final class AddressFormatterService {
	private static final Pattern TITLE_RE = Pattern.compile("(?i)^\\s*(FRAU|HERR|MADAME|MONSIEUR|MR|MS|M|MME)\\s*$");
	private static final Pattern ZIP_RE = Pattern.compile("^(\\d{4})(?:[-\\s]?(\\d{2}))?$");
	private static final Pattern HOUSE_RE = Pattern.compile(
			"(?ix)^\\s*(?<street>.+?)\\s*(?<number>[1-9]\\d{0,3}(?:(?:(?:bis|ter|quater|quinquies)|[A-Za-z]))?(?:/[1-9]\\d{0,3})?)\\s*$");
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

			if (streetOrPoBox == null) {
				Matcher poBoxMatcher = POSTAL_BOX_RE.matcher(trimmed);
				if (poBoxMatcher.matches()) {
					streetOrPoBox = trimmed;
					address = AddressLine.poBox(trimmed);
					continue;
				}

				Matcher hosueMatcher = HOUSE_RE.matcher(trimmed);
				if (hosueMatcher.matches()) {
					String street = hosueMatcher.group("street").trim();
					String houseNumber = hosueMatcher.group("number").trim();
					streetOrPoBox = street;
					address = AddressLine.street(street, houseNumber);
					continue;
				}
			}

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
				compl2Value, address, postal, cityValue, country);
	}

	public static long saveStructuredAddress(String table, StructuredAddress addr, SqlClient client)
			throws SQLException {
		String street = null;
		String houseNumber = null;
		String poBox = null;
		if (addr.address() instanceof AddressLine.Street streetLine) {
			street = streetLine.street();
			houseNumber = streetLine.houseNumber();
		} else if (addr.address() instanceof AddressLine.PoBox poBoxLine) {
			poBox = poBoxLine.boxNumber();
		}

		String nextIdExpr = "(SELECT ISNULL(MAX(t.ID_FPR_PAYREL), 0)+1 FROM " + table + " t)";

		String sql = ("""
					INSERT INTO %s (
				                ID_FPR_PAYREL,
				                FPR_PAYEMENT_DOMAIN,
				                FPR_ACCOUNT_OWNER_NAME,
				                FPR_ACCOUNT_OWNER_ADRESS_LINE1,
				                FPR_ACCOUNT_OWNER_ADRESS_LINE2,
				                FPR_STREET,
				                FPR_BUILDING_NUMBER,
				                FPR_POST_CODE,
				                FPR_TOWN_NAME,
				                FPR_ACCOUNT_OWNER_ADDRESS_COUNTRY,
				                FPR_ACCOUNT_TYPE,
				                FPR_ACCOUNT_NO,
				                FPR_CURRENCY,
				                FPR_PAYMENT_POOL,
				                FPR_ACCOUNT_NO_REF,
				                FPR_VALIDITY_START,
				                FPR_VALIDITY_END,
				                FPR_STATE,
				                FPR_SOURCE,
				                FPR_VALID,
				                FPR_USR_LOG_I,
				                FPR_DTE_LOG_I,
				                FPR_USR_LOG_U,
				                FPR_DTE_LOG_U,
				                OLD_TBL_ID,
				                OLD_ID_ADRESSE,
				                RIP_PERSON_ID,
				                RIP_PERSON_BPC_ID,
				                PAC_PAYEMENT_ADRESS_ID,
				                PAC_VERSION_ADR
				            )
				            SELECT
				                %s,
				                'FCF',
				                %s,
				                %s,
				                %s,
				                %s,
				                %s,
				                %s,
				                %s,
				                %s,
				                'TRAN_CH',
				                'CH00',
				                'CHF',
				                0,
				                NULL,
				                GETDATE(),
				                NULL,
				                'ACTIVE',
				                'OTH',
				                1,
				                'FORMAT',
				                GETDATE(),
				                'FORMAT',
				                GETDATE(),
				                %s,
				                NULL,
				                0,
				                0,
				                NULL,
				                NULL
				""").formatted(table, nextIdExpr, SqlClient.nullableQuoted(addr.name()),
				SqlClient.nullableQuoted(addr.compl1()), SqlClient.nullableQuoted(addr.compl2()),
				SqlClient.nullableQuoted(poBox != null ? poBox : street), SqlClient.nullableQuoted(houseNumber),
				addr.postal().code(), SqlClient.nullableQuoted(addr.city()), SqlClient.nullableQuoted(addr.country()),
				SqlClient.nullableQuoted(addr.id()));
		try {
			return client.executeNonQuery(sql);
		} catch (Exception e) {
			System.out.println("An error happened: " + e + "\nRequest: " + sql);
			return -1;
		}
	}

	public static long updateAddrPayId(String table, String idField, String idRefField, String refTable,
			String refIdField, String refLinkField, String refMainId, SqlClient client) throws SQLException {
		String sql = ("""
				UPDATE %s
				SET %s = (
					SELECT ad.%s
					FROM %s ad
					WHERE ad.%s = '%s'
				)
				WHERE %s = '%s'
				""").formatted(table, idRefField, refIdField, refTable, refLinkField, SqlClient.sqlQuote(refMainId),
				idField, SqlClient.sqlQuote(refMainId));
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