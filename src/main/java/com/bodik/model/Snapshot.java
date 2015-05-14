package com.bodik.model;

import org.json.JSONArray;
import org.json.JSONObject;

public class Snapshot {
	private String rowkey;
	private String userId;
	private String type;
	private String data;
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

	public Snapshot(String rowkey, String userId, String data, String type) {
		this.rowkey = rowkey;
		this.data = data;
		this.userId = userId;
		this.type = type;
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

	public String getData() {
		return this.data;
	}

	public void setData(String data) {
		this.data = data;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		JSONObject result = new JSONObject();
		if (data.toString().charAt(0) == '{'
				&& data.toString().charAt(data.toString().length() - 1) == '}') {
			result.put("data", new JSONObject(this.data));
		} else if (data.toString().charAt(0) == '['
				&& data.toString().charAt(data.toString().length() - 1) == ']') {
			result.put("data", new JSONArray(this.data));
		} else {
			result.put("data", this.data);
		}
		result.put("type", this.type);
		result.put("userId", this.userId);
		result.put("rowkey", this.rowkey);
		result.put("timestamp", this.timestamp);
		return result.toString();
	}

}