package dev.allanbrunner.addressFormatter.util;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class EnvLoader {
    private EnvLoader() {}

    public static Env loadDefault() throws IOException {
        Path dotEnv = Path.of(".env");
        Map<String, String> values = new HashMap<>();
		if (Files.exists(dotEnv)) {
			values.putAll(parseFile(dotEnv));
		}
		return new Env(values);
    }

	private static Map<String, String> parseFile(Path path) throws IOException {
		Map<String, String> values = new HashMap<>();
		for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
			String trimmed = line.trim();
			if (trimmed.isEmpty() || trimmed.startsWith("#")) {
				continue;
			}

			int equalsIndex = trimmed.indexOf('=');
			if (equalsIndex <= 0) {
				continue;
			}

			String key = trimmed.substring(0, equalsIndex).trim();
			String value = trimmed.substring(equalsIndex + 1).trim();
			if (value.startsWith("\"") && value.endsWith("\"") && value.length() >= 2) {
				value = value.substring(1, value.length() - 1);
			}
			values.put(key, value);
		}
		return values;
	}
}
