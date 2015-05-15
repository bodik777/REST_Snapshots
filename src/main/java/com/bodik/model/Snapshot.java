package com.bodik.model;

import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

public class Snapshot {
	private String rowkey;
	private String userId;
	private String type;

	private Object data;
	private Long timestamp;

	public Snapshot() {
	}

	public Snapshot(String rowkey, String userId, String data, String type,
			Long timestamp) {
		this.rowkey = rowkey;
		this.data = data;
		this.userId = userId;
		this.type = type;
		this.timestamp = timestamp;
	}

	public String getRowkey() {
		return rowkey;
	}

	public void setRowkey(String rowkey) {
		this.rowkey = rowkey;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public Object getData() {
		return this.data;
	}

	public void setData(Object data) {
		try {
			this.data = new ObjectMapper().writeValueAsString(data);
		} catch (JsonProcessingException e) {
			this.data = data;
		}
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder().append("{\"rowkey\":\"")
				.append(this.rowkey).append("\",\"userId\":\"")
				.append(this.userId).append("\",\"type\":\"").append(type)
				.append("\",\"data\":").append(this.data)
				.append(",\"timestamp\":\"").append(this.timestamp)
				.append("\"}");
		return sb.toString();
	}

}