package dev.allanbrunner.addressFormatter;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.function.Supplier;

import dev.allanbrunner.addressFormatter.address.AddressFormatterService;
import dev.allanbrunner.addressFormatter.address.StructuredAddress;
import dev.allanbrunner.addressFormatter.address.UnstructuredAddress;
import dev.allanbrunner.addressFormatter.db.SqlClient;
import dev.allanbrunner.addressFormatter.db.TableSql;
import dev.allanbrunner.addressFormatter.db.table.AddressFormattedTable;
import dev.allanbrunner.addressFormatter.db.table.AddressLinkTable;
import dev.allanbrunner.addressFormatter.db.table.AddressRawTable;
import dev.allanbrunner.addressFormatter.db.table.AddressRefTable;
import dev.allanbrunner.addressFormatter.db.table.type.Types;
import dev.allanbrunner.addressFormatter.util.Env;
import dev.allanbrunner.addressFormatter.util.EnvLoader;

public class Main {
	private static final int BATCH_SIZE = 200;
	private static final DateTimeFormatter SQL_DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");

	private Main() {}

	public static void main(String[] args) {
		try {
			Env env = EnvLoader.loadDefault();

			String server = env.require("DB_SERVER");
			String port = env.require("DB_PORT");
			String name = env.require("DB_NAME");
			String user = env.require("DB_USER");
			String pwd = env.require("DB_PASS");

			try (SqlClient client = new SqlClient(server, port, name, user, pwd, BATCH_SIZE)) {
				AddressRawTable rawTable = new AddressRawTable.Builder().name("FCF_DEMANDS")
						.id(b -> b.meta("dbName", "TOP 5 IDDEMAND")).line(1, 64, b -> b.meta("dbName", "RECEIVER1"))
						.line(2, 64, b -> b.meta("dbName", "RECEIVER2")).line(3, 64, b -> b.meta("dbName", "RECEIVER3"))
						.line(4, 64, b -> b.meta("dbName", "RECEIVER4")).line(5, 64, b -> b.meta("dbName", "RECEIVER5"))
						.iban(21, b -> b.meta("dbName", "IBAN")).accountOwner(-1, b -> b.meta("dbName", "ACCNTHOLDER"))
						.build();

				String formattedAddressTableName = "Addresses_TEMP";
				AddressFormattedTable formattedTable = new AddressFormattedTable.Builder()
						.name(formattedAddressTableName)
						.id(b -> b.meta("dbName", "ID_FPR_PAYREL").meta("value",
								"(SELECT ISNULL(MAX(t.ID_FPR_PAYREL), 0)+1 FROM " + formattedAddressTableName + " t)"))
						.oldId(b -> b.meta("dbName", "OLD_TBL_ID"))
						.ownerName(b -> b.meta("dbName", "FPR_ACCOUNT_OWNER_NAME"))
						.compl1(b -> b.meta("dbName", "FPR_ACCOUNT_OWNER_ADDRESS_LINE1"))
						.compl2(b -> b.meta("dbName", "FPR_ACCOUNT_OWNER_ADDRESS_LINE2"))
						.street(b -> b.meta("dbName", "FPR_STREET"))
						.houseNumber(b -> b.meta("dbName", "FPR_BUILDING_NUMBER"))
						.postalCodeLong(b -> b.meta("dbName", "FPR_POST_CODE"))
						.city(b -> b.meta("dbName", "FPR_TOWN_NAME"))
						.country(b -> b.meta("dbName", "FPR_ACCOUNT_OWNER_ADDRESS_COUNTRY").meta("default", "CH"))

						.add("paymentDomain", Types.NVARCHAR(20),
								b -> b.meta("dbName", "FPR_PAYEMENT_DOMAIN").meta("value", "FCF"))
						.add("accountType", Types.NVARCHAR(10),
								b -> b.meta("dbName", "FPR_ACCOUNT_TYPE").meta("value", "TRAN_CH"))
						.add("iban", Types.NVARCHAR(40), b -> b.meta("dbName", "FPR_ACCOUNT_NO").meta("default", ""))
						.add("currency", Types.NVARCHAR(3), b -> b.meta("dbName", "FPR_CURRENCY").meta("value", "CHF"))
						.add("paymentPool", Types.DEC(1, 0),
								b -> b.meta("dbName", "FPR_PAYMENT_POOL").meta("value", "0"))
						.add("validityStart", Types.DATE(),
								b -> b.required(true).meta("dbName", "FPR_VALIDITY_START").meta("value",
										(Supplier<String>) () -> LocalDate.now().format(SQL_DATE_FMT)))
						.add("validityEnd", Types.DATE(), b -> b.meta("dbName", "FPR_VALIDITY_END"))
						.add("state", Types.NVARCHAR(10),
								b -> b.required(true).meta("dbName", "FPR_STATE").meta("value", "ACTIVE"))
						.add("source", Types.NVARCHAR(10), b -> b.meta("dbName", "FPR_SOURCE").meta("value", "OTH"))
						.add("valid", Types.DEC(1, 0), b -> b.meta("dbName", "FPR_VALID").meta("value", "1"))
						.add("userLogInsert", Types.NVARCHAR(15),
								b -> b.required(true).meta("dbName", "FPR_USR_LOG_I").meta("value", "FORMAT"))
						.add("dateLogInsert", Types.DATE(),
								b -> b.required(true).meta("dbName", "FPR_DTE_LOG_I").meta("value",
										(Supplier<String>) () -> LocalDate.now().format(SQL_DATE_FMT)))
						.add("userLogUpdate", Types.NVARCHAR(15),
								b -> b.required(true).meta("dbName", "FPR_USR_LOG_U").meta("value", "FORMAT"))
						.add("dateLogUpdate", Types.DATE(),
								b -> b.required(true).meta("dbName", "FPR_DTE_LOG_U").meta("value",
										(Supplier<String>) () -> LocalDate.now().format(SQL_DATE_FMT)))
						.add("oldIdAddress", Types.DEC(15, 0),
								b -> b.meta("dbName", "OLD_ID_ADRESSE").meta("default", "0"))
						.add("ripPersonId", Types.DEC(10, 0),
								b -> b.meta("dbName", "RIP_PERSON_ID").meta("default", "0"))
						.add("ripPersonBpcId", Types.DEC(10, 0), b -> b.meta("dbName", "RIP_PERSON_BPC_ID"))
						.add("pacPaymentAddressId", Types.DEC(10, 0), b -> b.meta("dbName", "PAC_PAYEMENT_ADRESS_ID"))
						.add("pacVersionAddr", Types.DEC(4, 0), b -> b.meta("dbName", "PAC_VERSION_ADR")).build();

				System.out.println(TableSql.selectTable(rawTable));
				AddressLinkTable linkTable = new AddressLinkTable.Builder().name("FCF_TEMP_DEMANDS")
						.idOriginal(b -> b.meta("dbName", "IDDEMAND"))
						.idReferenced(b -> b.meta("dbName", "PAY_ADDR_ID")).build();

				AddressRefTable refTable = new AddressRefTable.Builder().name(formattedAddressTableName)
						.id(b -> b.meta("dbName", "ID_FPR_PAYREL")).oldId(b -> b.meta("dbName", "OLD_TBL_ID")).build();

				List<UnstructuredAddress> unstructuredAddresses = AddressFormatterService
						.getUnstructuredAddresses(rawTable, client);

				client.executeNonQuery(TableSql.createTable(formattedTable));

				Integer idx = 0;
				for (UnstructuredAddress unstructuredAddress : unstructuredAddresses) {
					idx += 1;
					if (idx % 25 == 0)
						System.out.println("Processing address " + idx + " / " + unstructuredAddresses.size());

					StructuredAddress structuredAddress = AddressFormatterService.format(unstructuredAddress);

					client.executeNonQuery(TableSql.insertSql(formattedTable, structuredAddress.toMap()));

					AddressFormatterService.updateAddrPayId(linkTable, refTable, structuredAddress.id(), client);
				}
			}
		} catch (Exception e) {
			System.err.println("Application failed: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
}