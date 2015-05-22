package com.bodik.dao;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.bodik.service.HBaseConnection;

public class DAO {
	protected Connection connection;

	protected DAO() {
		try {
			connection = ConnectionFactory.createConnection(HBaseConnection
					.getConf());
		} catch (IOException e) {
			Logger.getLogger(DAO.class).error("Could not connect to HBase!", e);
		}
	}

	protected Scan getScaner(String startRow, String stopRow, Long minStamp,
			Long maxStamp) {
		Scan s = null;
		try {
			s = new Scan();
			if (minStamp != null && maxStamp != null) {
				s.setTimeRange(minStamp, maxStamp);
			} else if (minStamp == null && maxStamp != null) {
				s.setTimeRange(0L, maxStamp);
			} else if (maxStamp == null && minStamp != null) {
				s.setTimeRange(minStamp, 9223372036854775807L);
			}
			if (startRow != null) {
				s.setStartRow(Bytes.toBytes(startRow));
			}
			if (stopRow != null) {
				s.setStopRow(Bytes.toBytes(stopRow));
			}
		} catch (IOException e) {
			Logger.getLogger(DAO.class).error("Failed to extract data!", e);
		}
		return s;
	}

	protected FilterList getFilter(HashMap<String, String> tags) {
		FilterList flMaster = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		for (Entry<String, String> tag : tags.entrySet()) {
			StringBuilder sb = new StringBuilder("|");
			sb.append(Integer.toHexString(tag.getKey().hashCode())).append(
					Integer.toHexString(tag.getValue().hashCode()));
			Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
					new SubstringComparator(sb.toString()));
			flMaster.addFilter(filter);
		}
		return flMaster;
	}

	protected Long getMaxTimestamp(Result rr) {
		Long times = 0L;
		for (Cell cell : rr.rawCells()) {
			if (times < cell.getTimestamp()) {
				times = cell.getTimestamp();
			}
		}
		return times;
	}
}
