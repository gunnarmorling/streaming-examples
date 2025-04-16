package dev.morling.demos.partialevents;

import java.util.Map;

public record KafkaRecord(SchematizedKey key, SchematizedChangeDataEvent value) {
}
