package com.nordstrom.kafka.connect.sqs.format.json;

import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.HashMap;

public class JsonFormat {
    JsonConverter converter;
    HashMap<String, String> jsonConverterConfig;

    public JsonFormat() {
        jsonConverterConfig = new HashMap<>();
        jsonConverterConfig.put("schemas.enable", "false");
        jsonConverterConfig.put("converter.type", "value");

        converter = new JsonConverter();
        converter.configure(jsonConverterConfig);
    }

    public String format(SinkRecord record) {
        return new String(converter.fromConnectData(record.topic(),record.valueSchema(), record.value()));
    }
}
