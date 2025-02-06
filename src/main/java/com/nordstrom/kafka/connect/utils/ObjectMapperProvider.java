package com.nordstrom.kafka.connect.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.kafka.connect.data.Struct;

public class ObjectMapperProvider {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    static {
        SimpleModule module = new SimpleModule();
        module.addSerializer(Struct.class, new StructSerializer());
        objectMapper.registerModule(module);
    }

    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}