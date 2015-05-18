package com.bodik.model;

import java.io.IOException;

import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.ObjectCodec;
import org.apache.htrace.fasterxml.jackson.databind.DeserializationContext;
import org.apache.htrace.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;

public class SnapshotDeserializer extends JsonDeserializer<Snapshot> {
	@Override
	public Snapshot deserialize(JsonParser jsonParser,
			DeserializationContext deserializationContext) throws IOException {
		ObjectCodec oc = jsonParser.getCodec();
		JsonNode node = oc.readTree(jsonParser);
		Snapshot sn = new Snapshot(node.get("rowkey").textValue(), node.get(
				"userId").textValue(), node.get("data").toString(), node.get(
				"type").textValue());
		return sn;
	}
}