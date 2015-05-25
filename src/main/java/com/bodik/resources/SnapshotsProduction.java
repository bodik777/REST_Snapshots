package com.bodik.resources;

import java.util.ArrayList;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.htrace.fasterxml.jackson.core.JsonProcessingException;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;

import com.bodik.dao.SnapshotsDao;
import com.bodik.model.Snapshot;

@Path("/snapshots")
public class SnapshotsProduction {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String getAll(@QueryParam("startRow") String startRow,
			@QueryParam("stopRow") String stopRow,
			@QueryParam("minStamp") Long minStamp,
			@QueryParam("maxStamp") Long maxStamp,
			@QueryParam("fTagK") ArrayList<String> fTagsK,
			@QueryParam("fTagV") ArrayList<String> fTagsV)
			throws JsonProcessingException {
		return new ObjectMapper().writeValueAsString(new SnapshotsDao().getAll(
				startRow, stopRow, minStamp, maxStamp, fTagsK, fTagsV));
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getById(@PathParam("id") String id)
			throws JsonProcessingException {
		Snapshot snapshot = new SnapshotsDao().getById(id);
		if (snapshot == null) {
			return Response.status(404).build();
		}
		return new ObjectMapper().writeValueAsString(snapshot);
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRow(String request) {
		Snapshot snapshot;
		try {
			snapshot = new ObjectMapper().readValue(request, Snapshot.class);
			if (snapshot.getRowkey() != null && !snapshot.getRowkey().isEmpty()) {
				new SnapshotsDao().putToTable(snapshot);
			}
		} catch (Exception e) {
			Logger.getLogger(SnapshotsDao.class)
					.error("Error adding entry!", e);
			return Response.status(400).build();
		}
		return Response.status(200).build();
	}

}