package com.bodik.model;

import java.io.IOException;
import java.util.HashMap;

import org.apache.htrace.fasterxml.jackson.core.JsonParser;
import org.apache.htrace.fasterxml.jackson.core.ObjectCodec;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.DeserializationContext;
import org.apache.htrace.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.htrace.fasterxml.jackson.databind.JsonNode;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class SnapshotDeserializer extends JsonDeserializer<Snapshot> {
	@Override
	public Snapshot deserialize(JsonParser jsonParser,
			DeserializationContext deserializationContext) throws IOException {
		ObjectCodec oc = jsonParser.getCodec();
		JsonNode node = oc.readTree(jsonParser);

		HashMap<String, String> tags = new ObjectMapper().readValue(
				node.get("tags").toString(),
				new TypeReference<HashMap<String, String>>() {
				});
		Snapshot sn = new Snapshot(node.get("userId").textValue(), node.get(
				"data").toString(), tags, node.get("type").textValue());
		return sn;
	}
}