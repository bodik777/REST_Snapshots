package com.bodik.resources;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import com.bodik.dao.SnapshotsDao;
import com.bodik.model.Snapshot;

@Path("snapshots")
public class SnapshotsProduction {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getAll(@QueryParam("startRow") String startRow,
			@QueryParam("stopRow") String stopRow,
			@QueryParam("minStamp") Long minStamp,
			@QueryParam("maxStamp") Long maxStamp,
			@QueryParam("fTag1") String fTag1, @QueryParam("fTag2") String fTag2) {
		return new SnapshotsDao().getAll(startRow, stopRow, minStamp, maxStamp,
				fTag1, fTag2).toString();
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getById(@PathParam("id") String id) {
		Snapshot snapshot = new SnapshotsDao().getById(id);
		if (snapshot == null) {
			return Response.status(404).build();
		}
		return snapshot.toString();
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRow(InputStream incomingData) {
		StringBuilder requestData = new StringBuilder();
		try {
			BufferedReader in = new BufferedReader(new InputStreamReader(
					incomingData));
			String line = null;
			while ((line = in.readLine()) != null) {
				requestData.append(line);
			}
		} catch (Exception e) {
			Logger.getLogger(SnapshotsProduction.class).error(
					"Error getting query!", e);
		}
		String request = requestData.toString();
		try {
			JSONObject obj = new JSONObject(request);
			if (obj.has("rowkey") && obj.has("userId") && obj.has("data")
					&& obj.has("type")) {
				new SnapshotsDao().putToTable(new Snapshot(obj.get("rowkey")
						.toString(), obj.get("userId").toString(), obj.get(
						"data").toString(), obj.get("type").toString()));
				return Response.status(200).build();
			}
		} catch (JSONException e) {
			return Response.status(400).build();
		}
		return Response.status(400).build();
	}

}