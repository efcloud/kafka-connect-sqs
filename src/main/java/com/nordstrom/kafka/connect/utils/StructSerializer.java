package com.nordstrom.kafka.connect.utils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.kafka.connect.data.Struct;

import java.io.IOException;

public class StructSerializer extends JsonSerializer<Struct> {
    @Override
    public void serialize(Struct struct, JsonGenerator gen, SerializerProvider serializers) throws IOException {
        gen.writeStartObject();
        struct.schema().fields().forEach(field -> {
            try {
                gen.writeObjectField(field.name(), struct.get(field));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        gen.writeEndObject();
    }
}