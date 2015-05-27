package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.model.Snapshot;
import com.bodik.service.HBaseConnection;

public class SnapshotsDao extends DAO {
	private final String TABLE_SNAPSHOTS = "snapshotstest";
	private final String TABLE_TAGS = "snapshotstest-uids";
	private final String COLUMN_FAMILY = "cf";

	public SnapshotsDao() {
	}

	public ArrayList<Snapshot> getAll(String startRow, String stopRow,
			Long minStamp, Long maxStamp, ArrayList<String> fTagsK,
			ArrayList<String> fTagsV) {
		ArrayList<Snapshot> rows = new ArrayList<Snapshot>();
		ResultScanner scanner = null;

		try (Connection connection = ConnectionFactory
				.createConnection(HBaseConnection.getConf());
				Table tableSnapshots = connection.getTable(TableName
						.valueOf(TABLE_SNAPSHOTS));) {

			Scan s = getScanner(startRow, stopRow, minStamp, maxStamp);
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("data"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("userId"));
			s.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("type"));

			if (!fTagsK.isEmpty() && !fTagsV.isEmpty()) {
				FilterList flMaster = getFilter(fTagsK, fTagsV);
				s.setFilter(flMaster);
			}

			scanner = tableSnapshots.getScanner(s);
			for (Result rr : scanner) {
				rows.add(new Snapshot(Bytes.toString(rr.getRow()), Bytes
						.toString(rr.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("userId"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("data"))), Bytes.toString(rr
						.getValue(Bytes.toBytes(COLUMN_FAMILY),
								Bytes.toBytes("type"))), getMaxTimestamp(rr)));
			}
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class)
					.error("Could not connect to HBase OR Could not connect to table OR Failed to extract data!",
							e);
		} finally {
			if (scanner != null) {
				scanner.close();
			}
		}
		return rows;
	}

	public Snapshot getById(String id) {
		Snapshot snapshot = null;
		Get query = new Get(id.getBytes());
		try (Connection connection = ConnectionFactory
				.createConnection(HBaseConnection.getConf());
				Table tableSnapshots = connection.getTable(TableName
						.valueOf(TABLE_SNAPSHOTS));) {
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
			Logger.getLogger(SnapshotsDao.class)
					.error("Could not connect to HBase OR Could not connect to table OR Failed to extract data!",
							e);
		}
		return snapshot;
	}

	public void putToTable(Snapshot snapshot) throws IOException {
		try (Connection connection = ConnectionFactory
				.createConnection(HBaseConnection.getConf());
				Table tableSnapshots = connection.getTable(TableName
						.valueOf(TABLE_SNAPSHOTS));
				Table tableTags = connection.getTable(TableName
						.valueOf(TABLE_TAGS));) {

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
		} catch (IOException e) {
			Logger.getLogger(SnapshotsDao.class)
					.error("Could not connect to HBase OR Could not connect to table OR Failed to insert data!",
							e);
		}
	}

}