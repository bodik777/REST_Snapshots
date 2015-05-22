package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.htrace.fasterxml.jackson.core.type.TypeReference;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import com.bodik.model.Snapshot;

public class SnapshotsDao extends DAO {
	private Table tableSnapshots = null;
	private Table tableTags = null;
	private final String TABLE_SNAPSHOTS = "snapshotstest";
	private final String TABLE_TAGS = "snapshotstest-uids";
	private final String COLUMN_FAMILY = "cf";

	public SnapshotsDao() {
		super();
		try {
			tableSnapshots = connection.getTable(TableName
					.valueOf(TABLE_SNAPSHOTS));
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<Snapshot> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, String fTags) {
		ArrayList<Snapshot> rows = new ArrayList<Snapshot>();
		try {
			try {
				Scan s = getScaner(startRow, stopRow, minStamp, maxStamp);
				s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("data"));
				s.addColumn(Bytes.toBytes(COLUMN_FAMILY),
						Bytes.toBytes("userId"));
				s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("type"));

				HashMap<String, String> tags = new HashMap<String, String>();
				ObjectMapper mapper = new ObjectMapper();
				if (fTags != null) {
					tags = mapper.readValue(fTags,
							new TypeReference<HashMap<String, String>>() {
							});
					FilterList flMaster = getFilter(tags);
					s.setFilter(flMaster);
				}

				ResultScanner scanner = tableSnapshots.getScanner(s);
				try {
					for (Result rr : scanner) {
						rows.add(new Snapshot(Bytes.toString(rr.getRow()),
								Bytes.toString(rr.getValue(
										Bytes.toBytes(COLUMN_FAMILY),
										Bytes.toBytes("userId"))), Bytes
										.toString(rr.getValue(
												Bytes.toBytes(COLUMN_FAMILY),
												Bytes.toBytes("data"))), Bytes
										.toString(rr.getValue(
												Bytes.toBytes(COLUMN_FAMILY),
												Bytes.toBytes("type"))),
								getMaxTimestamp(rr)));
					}
				} finally {
					scanner.close();
				}

			} finally {
				if (tableSnapshots != null)
					tableSnapshots.close();
				connection.close();
			}
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class).error(
					"Failed to extract data!", e);
		}
		return rows;
	}

	// public Snapshot getById(String id) {
	// Snapshot snapshot = null;
	// Get query = new Get(id.getBytes());
	// try {
	// try {
	// Result res = tableSnapshots.get(query);
	// if (!res.isEmpty()) {
	// snapshot = new Snapshot(Bytes.toString(res.getRow()),
	// Bytes.toString(res.getValue(
	// Bytes.toBytes(COLUMN_FAMILY),
	// Bytes.toBytes("userId"))),
	// Bytes.toString(res.getValue(
	// Bytes.toBytes(COLUMN_FAMILY),
	// Bytes.toBytes("data"))), Bytes.toString(res
	// .getValue(Bytes.toBytes(COLUMN_FAMILY),
	// Bytes.toBytes("type"))),
	// getMaxTimestamp(res));
	// }
	// } catch (IOException e) {
	// Logger.getLogger(SnapshotsDao.class).error(
	// "Failed to extract data!", e);
	// } finally {
	// // if (tableSnapshots != null) {
	// // tableSnapshots.close();
	// // }
	// connection.close();
	// }
	// } catch (IOException e) {
	// Logger.getLogger(SnapshotsDao.class).error(
	// "Failed to extract data!", e);
	// }
	// return snapshot;
	// }

	public Snapshot getById(String id) {
		Snapshot snapshot = null;
		Get query = new Get(id.getBytes());
		try {
			Result res = tableSnapshots.get(query);
			if (!res.isEmpty()) {
				snapshot = new Snapshot(Bytes.toString(res.getRow()),
						Bytes.toString(res.getValue(
								Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("userId"))), Bytes.toString(res
								.getValue(Bytes.toBytes(COLUMN_FAMILY),
										Bytes.toBytes("data"))),
						Bytes.toString(res.getValue(
								Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("type"))), getMaxTimestamp(res));
			}
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class).error(
					"Failed to extract data!", e);
		}
		return snapshot;
	}

	public void putToTable(Snapshot snapshot) {
		try {
			// try {
			tableTags = super.connection
					.getTable(TableName.valueOf(TABLE_TAGS));
			StringBuilder rowKey = new StringBuilder(snapshot.getRowkey());
			for (Entry<String, String> tag : snapshot.getTags().entrySet()) {
				String key = Integer.toHexString(tag.getKey().hashCode());
				String val = Integer.toHexString(tag.getValue().hashCode());
				rowKey.append("|").append(key).append(val);

				Put tagk = new Put(Bytes.toBytes(key));
				tagk.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
						Bytes.toBytes("tagk"), Bytes.toBytes(tag.getKey()));
				tableTags.put(tagk);
				Put tagv = new Put(Bytes.toBytes(val));
				tagv.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
						Bytes.toBytes("tagv"), Bytes.toBytes(tag.getValue()));
				tableTags.put(tagv);
			}

			Put sn = new Put(Bytes.toBytes(rowKey.toString()));
			sn.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("data"), Bytes.toBytes(snapshot.getData()));
			sn.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("userId"),
					Bytes.toBytes(snapshot.getUserId()));
			sn.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("type"), Bytes.toBytes(snapshot.getType()));
			tableSnapshots.put(sn);
			// } finally {
			// if (tableSnapshots != null)
			// tableSnapshots.close();
			// if (tableTags != null)
			// tableTags.close();
			// connection.close();
			// }
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class)
					.error("Error adding entry!", e);
		}
	}

}