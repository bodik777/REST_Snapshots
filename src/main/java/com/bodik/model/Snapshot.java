package com.bodik.model;

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
		try {
			return new ObjectMapper().readValue(this.data.toString(),
					Object.class);
		} catch (Exception e) {
			return this.data;
		}
	}

	public void setData(Object data) {
		this.data = data;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
}