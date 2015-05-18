package com.bodik.model;

import java.io.IOException;

import org.apache.htrace.fasterxml.jackson.core.JsonGenerator;
import org.apache.htrace.fasterxml.jackson.databind.JsonSerializer;
import org.apache.htrace.fasterxml.jackson.databind.SerializerProvider;

public class SnapshotSerializer extends JsonSerializer<Snapshot> {
	@Override
	public void serialize(Snapshot snapshot, JsonGenerator jsonGenerator,
			SerializerProvider serializerProvider) throws IOException {
		jsonGenerator.writeStartObject();

		jsonGenerator.writeStringField("rowkey", snapshot.getRowkey());
		jsonGenerator.writeStringField("userId", snapshot.getUserId());
		jsonGenerator.writeStringField("type", snapshot.getType());

		jsonGenerator.writeRaw(",\"data\":" + snapshot.getData());

		jsonGenerator.writeNumberField("timestamp", snapshot.getTimestamp());
		jsonGenerator.writeEndObject();
	}

}