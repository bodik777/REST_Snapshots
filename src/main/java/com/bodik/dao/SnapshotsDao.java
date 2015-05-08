package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.model.Snapshot;

public class SnapshotsDao extends DAO {
	private final String TABLE_SNAPSHOTS = "snapshots";
	private final String COLUMN_FAMILY = "cf";

	public SnapshotsDao() {
		super();
		try {
			tables = super.connection.getTable(TableName
					.valueOf(TABLE_SNAPSHOTS));
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class).error(
					"Could not connect to the table!", e);
		}
	}

	public ArrayList<Snapshot> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, String fTag1, String fTag2) {
		ArrayList<Snapshot> rows = new ArrayList<Snapshot>();
		try {
			Scan s = getScaner(startRow, stopRow, minStamp, maxStamp);
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("data"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("userId"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("type"));

			ArrayList<String> colList = new ArrayList<String>();
			ArrayList<String> valList = new ArrayList<String>();
			colList.add("Tags");
			valList.add(fTag1);
			colList.add("Tags");
			valList.add(fTag2);
			FilterList flMaster = getFilter(COLUMN_FAMILY, colList, valList);
			if (flMaster.hasFilterRow()) {
				s.setFilter(flMaster);
			}

			ResultScanner scanner = tables.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new Snapshot(Bytes.toString(rr.getRow()), Bytes
						.toString(rr.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("userId"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("data"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("type"))), getMaxTimestamp(rr)));
			}
			scanner.close();
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class).error(
					"Failed to extract data!", e);
		}
		return rows;
	}

	public Snapshot getById(String id) {
		Snapshot snapshot = null;
		try {
			Get query = new Get(id.getBytes());
			Result res = tables.get(query);
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
		Put p = new Put(Bytes.toBytes(snapshot.getRowkey()));
		try {
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("data"),
					Bytes.toBytes(snapshot.getData()));
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY),
					Bytes.toBytes("userId"),
					Bytes.toBytes(snapshot.getUserId()));
			p.addImmutable(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("type"),
					Bytes.toBytes(snapshot.getType()));
			tables.put(p);
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class)
					.error("Error adding entry!", e);
		}
	}

}