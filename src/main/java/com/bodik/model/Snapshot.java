package com.bodik.model;

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

	public String getData() {
		return data;
	}

	public void setData(String data) {
		this.data = data;
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

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

}