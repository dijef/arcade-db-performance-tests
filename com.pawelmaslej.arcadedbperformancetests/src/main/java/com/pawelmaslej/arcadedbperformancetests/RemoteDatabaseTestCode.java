package com.pawelmaslej.arcadedbperformancetests;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import com.arcadedb.database.RID;
import com.arcadedb.graph.Vertex;
import com.arcadedb.graph.Vertex.DIRECTION;
import com.arcadedb.remote.RemoteDatabase;
import com.arcadedb.remote.RemoteMutableVertex;
import com.pawelmaslej.arcadedbperformancetests.test.EndpointData;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;
import com.pawelmaslej.arcadedbperformancetests.test.PredictionStructureInputData;
import com.pawelmaslej.arcadedbperformancetests.test.StructureData;
import com.pawelmaslej.arcadedbperformancetests.utils.Utils;

/**
 * @author Pawel Maslej
 * @since 31 Oct 2023
 */
public class RemoteDatabaseTestCode {

	RemoteDatabase rd;
	PerformanceTest test;
	boolean tx;

	public RemoteDatabaseTestCode(RemoteDatabase rd,
		PerformanceTest test,
		boolean tx) {
		this.rd = rd;
		this.test = test;
		this.tx = tx;
	}

	public Function<List<String>, List<String>> insertFunction() {
		return (jsonList) -> {
			List<RemoteMutableVertex> vertices = new ArrayList<>(jsonList.size());
			txOpen();
			try {
				for (var json : jsonList) {
					var v = rd.newVertex("inputstructure");
					v.set("json", json);
					v.save();
					vertices.add(v);
				}
			}
			finally {
				txCommit();
			}
			return vertices.stream().map(v -> v.getIdentity().toString()).toList();
		};
	}

	public Function<List<PredictionStructureInputData>, List<String>> predictFunction() {
		return (inputDataList) -> {
			List<RemoteMutableVertex> vertices = new ArrayList<>(inputDataList.size());
			for (var inputData : inputDataList) {
				txOpen();
				long start = Calendar.getInstance().getTimeInMillis();
				try {
					var isv = (Vertex) rd.lookupByRID(asRID(inputData.getInputStructureId()), true);
					var inputStructureJson = isv.getString("json");
					var psv = rd.newVertex("predictionstructure");
					psv.set("json", inputStructureJson);
					psv.set("alerts", Utils.listToString(inputData.getAlerts()));
					psv.save();

					var edge1 = psv.newEdge("e_endpoint", asRID(inputData.getEndpointId()), true);
					var edge2 = isv.newEdge("e_predictionstructure", psv, true);
					edge1.save();
					edge2.save();

					vertices.add(psv);
				}
				finally {
					try {
						txCommit();
					}
					catch (Exception e) {
						long end = Calendar.getInstance().getTimeInMillis();
						System.out.println("Time: " + (end - start));
						throw e;
					}
				}
			}
			return vertices.stream().map(v -> v.getIdentity().toString()).toList();
		};
	}

	public Function<String, String> createEndpointFunction() {
		return (String endpointName) -> {
			txOpen();
			var	v = rd.newVertex("endpoint");
			v.set("name", endpointName);
			v.save();
			txCommit();
			return v.getIdentity().toString();
		};
	}

	public Supplier<List<StructureData>> queryAndIterateJsonAllInputStructuresFunction() {
		return () -> {
			txOpen();
			try {
				//TODO it does not support more than 20000 returned records
				var rs = rd.command("sql", "SELECT FROM ?", "inputstructure");
				List<Vertex> inputStructuresV = new ArrayList<Vertex>();
				while (rs.hasNext()) {
					inputStructuresV.add(rs.next().getVertex().get());
				}
				System.out.println("Queried input structures: " + inputStructuresV.size());

				for (var v : inputStructuresV) {
					String json = v.getString("json");
					if (json == null || json.isBlank()) {
						throw new RuntimeException("Json not available");
					}
				}
				return inputStructuresV.stream().map(v -> new StructureData(v.getIdentity().toString(), v.getString("json"))).toList();
			}
			finally {
				txRollback();
			}
		};
	}

	public Supplier<Integer> queryAndIterateDataAllPredictionStructuresFunction() {
		return () -> {
			txOpen();
			try {
				var predictionStructuresV = rd.command("sql", "SELECT FROM ?", "predictionstructure").toVertices();
				int edgesCount = 0;
				int alertsCount = 0;
				for (var v : predictionStructuresV) {
					String json = v.getString("json");
					if (json == null || json.isBlank()) {
						throw new RuntimeException("Json not available");
					}
					var edges = v.getEdges(DIRECTION.OUT, "e_endpoint").iterator();
					while (edges.hasNext()) {
						edgesCount++;
						var ev = edges.next().getInVertex();
						String name = ev.getString("name");
						if (name == null || name.isBlank()) {
							throw new RuntimeException("Name not available");
						}
					}
					String alerts = v.getString("alerts");
					if (alerts == null || alerts.isBlank()) {
						throw new RuntimeException("Json not available");
					}
					var alertsList = Utils.listFromString(alerts);
					alertsCount += alertsList.size();
				}
				return predictionStructuresV.size() + edgesCount + alertsCount;
			}
			finally {
				txRollback();
			}
		};
	}

	public Runnable printStatsFunction() {
		return () -> {
			txOpen();
			try {
				var inputStructuresCount = ((Integer) rd.command("sql", "SELECT COUNT (*) FROM ?", "inputstructure").next().getProperty("COUNT(*)")).intValue();
				var predictionStructuresCount = ((Integer) rd.command("sql", "SELECT COUNT (*) FROM ?", "predictionstructure").next().getProperty("COUNT(*)")).intValue();
				var endpointCount = ((Integer) rd.command("sql", "SELECT COUNT (*) FROM ?", "endpoint").next().getProperty("COUNT(*)")).intValue();
				test.printStats(inputStructuresCount, predictionStructuresCount, endpointCount);
			}
			finally {
				txRollback();
			}
		};
	}

	public Supplier<List<EndpointData>> findAllEndpointsFunction() {
		return () -> {
			txOpen();
			try {
				var vertices = rd.command("sql", "SELECT FROM ?", "endpoint").toVertices();
				return vertices.stream().map(v -> new EndpointData(v.getIdentity().toString(), v.getString("name"))).toList();
			}
			finally {
				txRollback();
			}
		};
	}

	public Function<String, List<String>> findAllPredictionStructuresByEndpointIdFunction() {
		return id -> {
			txOpen();
			try {
				var ev = (Vertex) rd.lookupByRID(asRID(id));
				var edgesIter = ev.getEdges(DIRECTION.IN, "e_endpoint").iterator();
				List<String> ids = new LinkedList<String>();
				while (edgesIter.hasNext()) {
					var psv = edgesIter.next().getOutVertex();
					if (psv.getIdentity().toString().equals(ev.getIdentity().toString())) {
						throw new RuntimeException("Wrong vertex");
					}
					ids.add(psv.getIdentity().toString());
				}
				return ids;
			}
			finally {
				txRollback();
			}
		};
	}

	public void txOpen() {
		if (tx) {
			rd.begin();
		}
	}

	public void txCommit() {
		if (tx) {
			rd.commit();
		}
	}

	public void txRollback() {
		if (tx) {
			rd.rollback();
		}
	}

	private RID asRID(String id) {
		return new RID(null, id);
	}

	public Runnable deleteAllDataFunction() {
		return () -> {
			rd.begin();
			var vertices = rd.command("sql", "SELECT FROM predictionstructure").toVertices();
			for (var v : vertices) {
				rd.deleteRecord(v);
			}
			rd.commit();
			rd.begin();
			vertices = rd.command("sql", "SELECT FROM inputstructure").toVertices();
			for (var v : vertices) {
				rd.deleteRecord(v);
			}
			rd.commit();
			rd.begin();
			vertices = rd.command("sql", "SELECT FROM endpoint").toVertices();
			for (var v : vertices) {
				rd.deleteRecord(v);
			}
			rd.commit();
		};
	}
}