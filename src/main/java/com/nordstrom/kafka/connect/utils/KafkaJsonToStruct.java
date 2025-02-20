package com.nordstrom.kafka.connect.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Optional;

public class KafkaJsonToStruct {

    public static SchemaBuilder buildSchema(Map<String, Object> jsonMap) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        jsonMap.forEach((k, v) -> {
            if (v instanceof Map) {
                // Nested JSON object detected
                //noinspection unchecked
                SchemaBuilder nestedSchema = buildSchema((Map<String, Object>) v);
                schemaBuilder.field(k, nestedSchema.build());
            } else if (v instanceof Integer) {
                schemaBuilder.field(k, Schema.OPTIONAL_INT32_SCHEMA);
            } else if (v instanceof Long) {
                schemaBuilder.field(k, Schema.OPTIONAL_INT64_SCHEMA);
            } else if (v instanceof Float) {
                schemaBuilder.field(k, Schema.OPTIONAL_FLOAT32_SCHEMA);
            } else if (v instanceof Double) {
                schemaBuilder.field(k, Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else if (v instanceof Boolean) {
                schemaBuilder.field(k, Schema.OPTIONAL_BOOLEAN_SCHEMA);
            } else if (v instanceof String) {
                Optional<java.sql.Timestamp> timestamp = parseTimestamp((String) v);
                if (timestamp.isPresent()) {
                    schemaBuilder.field(k, Timestamp.SCHEMA);
                } else {
                    schemaBuilder.field(k, Schema.OPTIONAL_STRING_SCHEMA);
                }
            } else {
                schemaBuilder.field(k, Schema.OPTIONAL_STRING_SCHEMA);
            }
        });
        return schemaBuilder;
    }

    public static Struct buildStruct(Map<String, Object> jsonMap, Schema schema) {
        Struct struct = new Struct(schema);
        jsonMap.forEach((k, v) -> {
            Schema fieldSchema = schema.field(k).schema();
            if (v instanceof Map) {
                // Recursive call for nested object.
                Struct nestedStruct = buildStruct((Map<String, Object>) v, fieldSchema);
                struct.put(k, nestedStruct);
            } else if (v instanceof String) {
                // Parse timestamps if possible.
                Optional<java.sql.Timestamp> timestamp = parseTimestamp((String) v);
                if (timestamp.isPresent()) {
                    struct.put(k, timestamp.get());
                } else {
                    struct.put(k, v);
                }
            } else {
                // basic types
                struct.put(k, v);
            }
        });
        return struct;
    }

    /**
     * Given a String, tries to convert it to one of the defined timestamp formats.
     *
     * @param value String to be parsed.
     * @return Optional of java.sql.Timestamp if the value could be parsed, empty otherwise.
     */
    public static Optional<java.sql.Timestamp> parseTimestamp(String value) {
        // List of supported date-time formatters
        DateTimeFormatter[] formatters = new DateTimeFormatter[]{
                DateTimeFormatter.ISO_INSTANT,
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of("UTC"))
        };

        for (DateTimeFormatter formatter : formatters) {
            try {
                Instant instant;
                if (formatter == DateTimeFormatter.ISO_INSTANT) {
                    instant = Instant.parse(value);
                } else {
                    LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
                    instant = localDateTime.atZone(ZoneId.of("UTC")).toInstant();
                }
                return Optional.of(java.sql.Timestamp.from(instant));
            } catch (DateTimeParseException e) {
                // Not throwing, only returning empty if no formatter could parse the value
            }
        }
        return Optional.empty();
    }

}