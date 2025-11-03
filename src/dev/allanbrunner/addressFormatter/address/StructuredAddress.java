package dev.allanbrunner.addressFormatter.address;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import dev.allanbrunner.addressFormatter.address.AddressLine.PoBox;
import dev.allanbrunner.addressFormatter.address.AddressLine.Street;

public record StructuredAddress(String id, String title, String name, String lastname, String firstname, String compl1,
		String compl2, AddressLine address, PostalCode postal, String city, String country, String iban,
		String accountOwner) {
	public StructuredAddress {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(address, "address");
		Objects.requireNonNull(postal, "postal");
		Objects.requireNonNull(city, "city");
		Objects.requireNonNull(country, "coutry");
	}

	public Optional<String> titleOpt() { return Optional.ofNullable(title); }

	public Optional<String> nameOpt() { return Optional.ofNullable(name); }

	public Optional<String> lastnameOpt() { return Optional.ofNullable(lastname); }

	public Optional<String> firstnameOpt() { return Optional.ofNullable(firstname); }

	public Optional<String> compl1Opt() { return Optional.ofNullable(compl1); }

	public Optional<String> compl2Opt() { return Optional.ofNullable(compl2); }

	public Optional<String> ibanOpt() { return Optional.ofNullable(iban); }

	public Optional<String> accountOwnerOpt() { return Optional.ofNullable(accountOwner); }

	public Map<String, Object> toMap() {
		Map<String, Object> structuredAddressMap = new HashMap<>();

		structuredAddressMap.put("oldId", id());
		structuredAddressMap.put("title", titleOpt().orElse(""));
		structuredAddressMap.put("name", nameOpt().orElse(""));
		structuredAddressMap.put("firstname", firstnameOpt().orElse(""));
		structuredAddressMap.put("lastname", lastnameOpt().orElse(""));
		structuredAddressMap.put("compl1", compl1Opt().orElse(""));
		structuredAddressMap.put("compl2", compl2Opt().orElse(""));

		if (address() instanceof Street) {
			structuredAddressMap.put("street", ((Street) address()).street());
			structuredAddressMap.put("houseNumber", ((Street) address()).houseNumber());
		} else {
			structuredAddressMap.put("poBoxNumber", ((PoBox) address()).boxNumber());
		}

		structuredAddressMap.put("postalCode", postal().code());
		structuredAddressMap.put("postalCodeSuffix", postal().suffix());
		structuredAddressMap.put("postalCodeLong",
				postal().code() + (postal().suffix() != null ? postal().suffix() : 0));

		structuredAddressMap.put("city", city());
		structuredAddressMap.put("country", country());

		structuredAddressMap.put("iban", ibanOpt().orElse(""));
		structuredAddressMap.put("accountOwner", accountOwnerOpt().orElse(""));

		return structuredAddressMap;
	}
}
