package com.nordstrom.kafka.connect.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.LinkedHashMap;
import java.util.Map;

public class StructSerializer extends JsonSerializer<Struct> {

    private static String timestampPattern;
    private static SimpleDateFormat dateFormat;

    public StructSerializer(String timestampPattern) {
        StructSerializer.timestampPattern = timestampPattern;
        dateFormat = new SimpleDateFormat(timestampPattern);
    }

    @Override
    public void serialize(Struct struct, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
        final Map<String, Object> result = new LinkedHashMap<>(struct.schema().fields().size());
        for (Field field : struct.schema().fields()) {
            Object value = struct.get(field);
            // Transform timestamp to defined format
            if (timestampPattern != null && field.schema().name() != null && field.schema().name().equals(Timestamp.LOGICAL_NAME)) {
                value = dateFormat.format(value);
            }
            result.put(field.name(), value);
        }
        jsonGenerator.writeObject(result);
    }
}