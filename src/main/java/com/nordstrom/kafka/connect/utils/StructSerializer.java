package com.nordstrom.kafka.connect.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.io.IOException;
import java.text.SimpleDateFormat;

public class StructSerializer extends JsonSerializer<Struct> {

    private static String timestampPattern;
    private static SimpleDateFormat dateFormat;

    public StructSerializer(String timestampPattern) {
        StructSerializer.timestampPattern = timestampPattern;
        dateFormat = new SimpleDateFormat(timestampPattern);
    }

    @Override
    public void serialize(Struct struct, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        struct.schema().fields().forEach(field -> {
            try {
                Object value = struct.get(field);

                // Transform timestamp to defined format
                if (timestampPattern != null && field.schema().name() != null && field.schema().name().equals(Timestamp.LOGICAL_NAME)) {
                    if (value instanceof java.util.Date) {
                        value = dateFormat.format((java.util.Date) value);
                    } else {
                        throw new IllegalArgumentException("Unsupported timestamp type: " + value.getClass());
                    }
                }

                // Recursively serialize nested objects
                if (value != null) {
                    if (value instanceof Struct) {
                        gen.writeFieldName(field.name());
                        serialize((Struct) value, gen, serializers);
                    } else {
                        gen.writeObjectField(field.name(), value);
                    }
                } else {
                    gen.writeObjectField(field.name(), null);
                }
            } catch (IOException e) {
                throw new ParseException("Failed to write field " + field.name() +
                        ", in Schema type: " + field.schema().name(), e);
            } catch (IllegalArgumentException e) {
                throw new ParseException("Failed to parse Timestamp field " + field.name() +
                        ", in Schema type: " + field.schema().name() +
                        " with value " + struct.get(field).toString(), e);
            }
        });
        gen.writeEndObject();
    }
}