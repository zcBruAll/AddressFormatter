package dev.allanbrunner.addressFormatter;

import java.util.List;

import dev.allanbrunner.addressFormatter.address.AddressFormatterService;
import dev.allanbrunner.addressFormatter.address.StructuredAddress;
import dev.allanbrunner.addressFormatter.address.UnstructuredAddress;
import dev.allanbrunner.addressFormatter.db.SqlClient;
import dev.allanbrunner.addressFormatter.util.Env;
import dev.allanbrunner.addressFormatter.util.EnvLoader;

public class Main {
	private static final int BATCH_SIZE = 200;

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
				List<UnstructuredAddress> unstructuredAddresses = AddressFormatterService.getUnstructuredAddresses(
						"FCF_DEMANDS", "TOP 15 IDDEMAND",
						new String[] { "RECEIVER1", "RECEIVER2", "RECEIVER3", "RECEIVER4", "RECEIVER5", }, client);

				String table = "Addresses_TEMP";
				client.ensureAddressTable(table);
				System.out.printf("Table %s existence ensured%n", table);

				for (UnstructuredAddress unstructured : unstructuredAddresses) {
					StructuredAddress structured = AddressFormatterService.format(unstructured);
					System.out.println("Address structured");

					AddressFormatterService.saveStructuredAddress(table, structured, client);
					System.out.println("Address saved in DB");

					AddressFormatterService.updateAddrPayId("FCF_TEMP_DEMANDS", "IDDEMAND", "PAY_ADDR_ID", table,
							"ID_FPR_PAYREL", "OLD_TBL_ID", structured.id(), client);
					System.out.println("Link updated in DB");
				}
			}
		} catch (Exception e) {
			System.err.println("Application failed: " + e.getMessage());
			e.printStackTrace(System.err);
			System.exit(1);
		}
	}
}