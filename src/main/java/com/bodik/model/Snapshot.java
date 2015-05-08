package com.bodik.model;

public class Snapshot {
	private String rowkey;
	private String data;
	private String userId;
	private String tags;
	private Long timestamp;

	public Snapshot() {
	}

	public Snapshot(String rowkey, String data, String userId, String tags,
			Long timestamp) {
		this.rowkey = rowkey;
		this.data = data;
		this.userId = userId;
		this.tags = tags;
		this.timestamp = timestamp;
	}

	public Snapshot(String rowkey, String data, String userId, String tags) {
		this.rowkey = rowkey;
		this.data = data;
		this.userId = userId;
		this.tags = tags;
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

	public String getTags() {
		return tags;
	}

	public void setTags(String tags) {
		this.tags = tags;
	}

	public Long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}

}