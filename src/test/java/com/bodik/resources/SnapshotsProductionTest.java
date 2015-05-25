package com.bodik.resources;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.jboss.resteasy.client.ClientRequest;
import org.jboss.resteasy.client.ClientResponse;
import org.junit.Test;

import com.bodik.service.HBaseConnection;

@SuppressWarnings("deprecation")
public class SnapshotsProductionTest {

	private final String ROOT_URL = "http://localhost:8080/REST_Snapshots/snapshots";
	private final String TABLE_NAME = "snapshotstest";

	@Test
	public final void testGetAll() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			ClientResponse<String> response = request.get(String.class);

			// BufferedReader br = new BufferedReader(new InputStreamReader(
			// new ByteArrayInputStream(response.getEntity().getBytes())));
			// String temp;
			// String res = "";
			// System.out.println("Request Transactions - getAll: Status: "
			// + response.getStatus() + "; Output ->");
			// while ((temp = br.readLine()) != null) {
			// res += temp;
			// }
			// System.out.println(res);
			System.out.println("Request Transactions - getAll: Status: "
					+ response.getStatus()
					+ (response.getStatus() == 200 ? "; Request success!"
							: "; Request failed!"));
			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public void testGetAllParams() {
		try {
			ClientRequest request = new ClientRequest(
					ROOT_URL
							+ "?minStamp=0&maxStamp=9223372036854775807&startRow=t&stopRow=u");
			// &fTagK=testk1&fTagV=testv1");
			ClientResponse<String> response = request.get(String.class);

			// BufferedReader br = new BufferedReader(new InputStreamReader(
			// new ByteArrayInputStream(response.getEntity().getBytes())));
			// String temp;
			// String res = "";
			// System.out.println("Request Transactions - getAllParams: Status: "
			// + response.getStatus() + "; Output ->");
			// while ((temp = br.readLine()) != null) {
			// res += temp;
			// }
			// System.out.println(res);
			System.out.println("Request Transactions - getAllParams: Status: "
					+ response.getStatus()
					+ (response.getStatus() == 200 ? "; Request success!"
							: "; Request failed!"));
			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testGetById() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL
					+ "/testRow|bbafd944bba63ce2");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			int status = response.getStatus();
			System.out.println("Request Transactions - getById: Status: "
					+ status + "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);
			assertTrue(status == 200);
			assertEquals(
					res,
					"{\"rowkey\":\"testRow|bbafd944bba63ce2\",\"userId\":\"TestUserID\",\"type\":\"snapshot\",\"data\":\"some data\",\"timestamp\":1432562849226}");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testCreateRowDataIsString() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			request.accept("application/json");

			String input = "{\"rowkey\":\"testRowString\",\"userId\":\"TestUserID\",\"type\":\"snapshot\",\"data\":\"Some data\",\"tags\":{\"testkey1\":\"testVal1\"}}";
			request.body("application/json", input);
			ClientResponse<String> response = request.post(String.class);
			int status = response.getStatus();
			System.out
					.println("Request Transactions - putToTable String data: Status: "
							+ status
							+ (status == 200 ? "; Request success!"
									: "; Request failed!"));
			assertTrue(status == 200);

			Connection connection = ConnectionFactory
					.createConnection(HBaseConnection.getConf());
			Table tables = connection.getTable(TableName.valueOf(TABLE_NAME));
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(
					Bytes.toBytes("testRowString|bbafd944bba63ce2"));
			list.add(del);
			tables.delete(list);
			assertTrue("Deleting success!", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testCreateRowDataIsJSONObject() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			request.accept("application/json");
			String input = "{\"rowkey\":\"testRowJSONObject\",\"userId\":\"TestUserID\",\"type\":\"snapshot\",\"data\":{\"key1\":\"val1\",\"test2\":\"val2\"},\"tags\":{\"testkey1\":\"testVal1\"}}";
			request.body("application/json", input);
			ClientResponse<String> response = request.post(String.class);
			int status = response.getStatus();
			System.out
					.println("Request Transactions - putToTable JSONObject data: Status: "
							+ status
							+ (status == 200 ? "; Request success!"
									: "; Request failed!"));
			assertTrue(status == 200);

			Connection connection = ConnectionFactory
					.createConnection(HBaseConnection.getConf());
			Table tables = connection.getTable(TableName.valueOf(TABLE_NAME));
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(
					Bytes.toBytes("testRowJSONObject|bbafd944bba63ce2"));
			list.add(del);
			tables.delete(list);
			assertTrue("Deleting success!", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testCreateRowDataIsJSONArray() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			request.accept("application/json");
			String input = "{\"rowkey\":\"testRowJsonArray\",\"userId\":\"TestUserID\",\"type\":\"snapshot\",\"data\":[{\"key1\":\"val1\",\"test2\":\"val2\"},{\"key11\":\"val11\",\"test12\":\"val12\"}],\"tags\":{\"testkey1\":\"testVal1\"}}";
			request.body("application/json", input);
			ClientResponse<String> response = request.post(String.class);
			int status = response.getStatus();
			System.out
					.println("Request Transactions - putToTable JSONArray data: Status: "
							+ status
							+ (status == 200 ? "; Request success!"
									: "; Request failed!"));
			assertTrue(status == 200);

			Connection connection = ConnectionFactory
					.createConnection(HBaseConnection.getConf());
			Table tables = connection.getTable(TableName.valueOf(TABLE_NAME));
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(
					Bytes.toBytes("testRowJsonArray|bbafd944bba63ce2"));
			list.add(del);
			tables.delete(list);
			assertTrue("Deleting success!", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
