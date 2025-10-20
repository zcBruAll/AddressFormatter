package dev.allanbrunner.addressFormatter.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public final class Env {
    private final Map<String, String> values;

    Env(Map<String, String> values) {
        this.values = new HashMap<>(values);
    }

    public String require(String key) {
        String fromEnv = System.getenv(key);
        if (fromEnv != null && !fromEnv.isBlank()) {
            return fromEnv.trim();
        }

        String fromFile = values.get(key);
        if (fromFile != null && !fromFile.isBlank()) {
            return fromFile.trim();
        }

        throw new IllegalStateException(key + " must be set");
    }

    Map<String, String> asMap() {
        return Collections.unmodifiableMap(values);
    }
}
