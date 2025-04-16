package dev.morling.demos.partialevents;

import java.util.Map;

public record SchematizedKey(Map<String, Object> schema, Map<String, Object> payload) {
}
