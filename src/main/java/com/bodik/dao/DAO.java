package com.bodik.dao;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class DAO {

	public DAO() {
	}

	protected Scan getScanner(String startRow, String stopRow, Long minStamp,
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
			Logger.getLogger(DAO.class)
					.error("Failed to configure scanner!", e);
		}
		return s;
	}

	protected FilterList getFilter(ArrayList<String> fTagsK,
			ArrayList<String> fTagsV) {
		FilterList flMaster = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		for (int i = 0; i < fTagsK.size(); i++) {
			StringBuilder sb = new StringBuilder("|");
			sb.append(Integer.toHexString(fTagsK.get(i).hashCode())).append(
					Integer.toHexString(fTagsV.get(i).hashCode()));
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
