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
	private final String TABLE_NAME = "snapshots";

	@Test
	public final void testGetAll() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			System.out.println("Request Transactions - getAll: Status: "
					+ response.getStatus() + "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);

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
							+ "?startRow=1&stopRow=999&minStamp=0&maxStamp=9223372036854775807");// &fCountry=United
																									// Kingdom&fProduct=Product1");
			ClientResponse<String> response = request.get(String.class);

			BufferedReader br = new BufferedReader(new InputStreamReader(
					new ByteArrayInputStream(response.getEntity().getBytes())));
			String temp;
			String res = "";
			System.out.println("Request Transactions - getAllParams: Status: "
					+ response.getStatus() + "; Output ->");
			while ((temp = br.readLine()) != null) {
				res += temp;
			}
			System.out.println(res);

			assertTrue(response.getStatus() == 200);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testGetById() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL
					+ "/3b628b665557990e3365fdc5f5a50008|45d2bfede10febaf");
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
					"{\"rowkey\":\"3b628b665557990e3365fdc5f5a50008|45d2bfede10febaf\",\"userId\":\"902074\",\"type\":\"snapshot\",\"data\":\"{\\\"location\\\":\\\"wpjxmrb\\\",\\\"place\\\":\\\"dnoe\\\",\\\"struct\\\":\\\"bxrgwulq\\\",\\\"param\\\":\\\"ttorbsod\\\"}\",\"timestamp\":1431000990656}");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Test
	public final void testCreateRow() {
		try {
			ClientRequest request = new ClientRequest(ROOT_URL);
			request.accept("application/json");
			String input = "{\"rowkey\":\"testRow\",\"userId\":\"TestUserID\",\"type\":\"snapshot\",\"data\":\"{\\\"key1\\\":\\\"val1\\\",\\\"test2\\\":\\\"val2\\\"}\"}";
			//{\"key1\":\"val1\",\"test2\":\"val2\"}
			request.body("application/json", input);

			ClientResponse<String> response = request.post(String.class);
			int status = response.getStatus();

			System.out.println("Request Transactions - putToTable: Status: "
					+ status
					+ (status == 200 ? "; Request success!"
							: "; Request failed!"));
			assertTrue(status == 200);

			Connection connection = ConnectionFactory
					.createConnection(HBaseConnection.getConf());
			Table tables = connection.getTable(TableName.valueOf(TABLE_NAME));
			List<Delete> list = new ArrayList<Delete>();
			Delete del = new Delete(Bytes.toBytes("testRow"));
			list.add(del);
			tables.delete(list);
			assertTrue("Deleting success!", true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
