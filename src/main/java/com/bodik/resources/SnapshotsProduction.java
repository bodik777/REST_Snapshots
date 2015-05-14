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

import com.bodik.dao.SnapshotsDao;
import com.bodik.model.Snapshot;

@Path("snapshots")
public class SnapshotsProduction {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public ArrayList<Snapshot> getAll(@QueryParam("startRow") String startRow,
			@QueryParam("stopRow") String stopRow,
			@QueryParam("minStamp") Long minStamp,
			@QueryParam("maxStamp") Long maxStamp,
			@QueryParam("fTag1") String fTag1, @QueryParam("fTag2") String fTag2) {
		return new SnapshotsDao().getAll(startRow, stopRow, minStamp, maxStamp,
				fTag1, fTag2);
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Object getById(@PathParam("id") String id) {
		Snapshot snapshot = new SnapshotsDao().getById(id);
		if (snapshot == null) {
			return Response.status(404).build();
		}
		return snapshot;
	}

	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createRow(Snapshot snapshot) {
		if (snapshot.getRowkey() != null && !snapshot.getRowkey().isEmpty()) {
			new SnapshotsDao().putToTable(snapshot);
			return Response.status(200).build();
		}
		return Response.status(400).entity(snapshot).build();
	}

}