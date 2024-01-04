package com.pawelmaslej.arcadedbperformancetests;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.pawelmaslej.arcadedbperformancetests.test.EndpointData;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;
import com.pawelmaslej.arcadedbperformancetests.test.PredictionStructureInputData;
import com.pawelmaslej.arcadedbperformancetests.test.StructureData;
import com.pawelmaslej.arcadedbperformancetests.utils.Utils;

/**
 * @author Pawel Maslej
 * @since 31 Oct 2023
 */
public class TraversalTinkerPopTestCode {

	GraphTraversalSource gts;
	PerformanceTest test;
	boolean tx;

	public TraversalTinkerPopTestCode(
		GraphTraversalSource gts,
		PerformanceTest test,
		boolean tx) {
		this.gts = gts;
		this.test = test;
		this.tx = tx;
	}

	public Function<List<String>, List<String>> insertFunction() {
		return (jsonList) -> {
			List<Object> ids = new ArrayList<Object>(jsonList.size());
			txOpen();
			try {
				for (var json : jsonList) {
					ids.add(gts.clone().addV("inputstructure").property("json", json).id().next());
				}
			}
			finally {
				txCommit();
			}
			return ids.stream().map(Object::toString).toList();
		};
	}

	public Function<List<PredictionStructureInputData>, List<String>> predictFunction() {
		return (inputDataList) -> {
			List<Object> ids = new ArrayList<>(inputDataList.size());
			for (var inputData : inputDataList) {
				txOpen();
				try {
					var vQueryList = gts.clone().V(inputData.getInputStructureId()).toList();
					var isv = vQueryList.get(0);
					var inputStructureJson = isv.value("json");

					var psv = gts.clone()
						.addV("predictionstructure")
						.as("psv")
						.property("json", inputStructureJson)
						.property("alerts", Utils.listToString(inputData.getAlerts()))
						.V(inputData.getEndpointId()).as("ev")
						.V(isv.id().toString()).as("isv")
						.addE("e_endpoint").from("psv").to("ev").outV()
						.addE("e_predictionstructure").from("isv").to("psv").inV()
						.id()
						.next();

					ids.add(psv);
				}
				finally {
					txCommit();
				}
			}
			return ids.stream().map(id -> id.toString()).toList();
		};
	}

	public Function<String, String> createEndpointFunction() {
		return (String endpointName) -> {
			txOpen();
			try {
				var t = gts.clone();
				var id = t.addV("endpoint").property("name", endpointName).id().next().toString();
				return id;
			}
			finally {
				txCommit();
			}
		};
	}

	public Supplier<List<StructureData>> queryAndIterateJsonAllInputStructuresFunction() {
		return () -> {
			txOpen();
			try {
				var inputStructuresV = gts.clone()
					.with("evaluationTimeout", 0)
					.V()
					.hasLabel("inputstructure")
					.toList();
				for (var v : inputStructuresV) {
					String json = v.value("json");
					if (json == null || json.isBlank()) {
						throw new RuntimeException("Json not available");
					}
				}
				return inputStructuresV.stream().map(v -> new StructureData(v.id().toString(), v.value("json"))).toList();
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
				var predictionStructuresV = gts.clone()
					.with("evaluationTimeout", 0)
					.V()
					.hasLabel("predictionstructure")
					.toList();
				int edgesCount = 0;
				int alertsCount = 0;
				for (var v : predictionStructuresV) {
					String json = v.value("json");
					if (json == null || json.isBlank()) {
						throw new RuntimeException("Json not available");
					}
					var eVertices = gts.clone().V(v.id()).outE("e_endpoint").inV().toList();
					for (var ev : eVertices) {
						edgesCount++;
						String name = ev.value("name");
						if (name == null || name.isBlank()) {
							throw new RuntimeException("Name not available");
						}
					}
					String alerts = v.value("alerts");
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
				var inputStructuresCount = gts.clone()
					.V()
					.hasLabel("inputstructure")
					.count().toList().get(0);
				var predictionStructuresCount = gts.clone()
						.V()
						.hasLabel("predictionstructure")
						.count().toList().get(0);
				var endpointCount = gts.clone()
						.V()
						.hasLabel("endpoint")
						.count().toList().get(0);
				test.printStats(inputStructuresCount.intValue(), predictionStructuresCount.intValue(), endpointCount.intValue());
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
				var vertices = gts.clone()
					.V()
					.hasLabel("endpoint")
					.toList();
				return vertices.stream().map(v -> new EndpointData(v.id().toString(), v.value("name"))).toList();
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
				var query = gts.clone()
					.with("evaluationTimeout", 0)
					.V(id)
					.inE("e_endpoint")
					.outV()
					.toList();
				return query.stream().map(Vertex::id).map(Object::toString).toList();
			}
			finally {
				txRollback();
			}
		};
	}

	public void txOpen() {
		if (tx) {
			gts.tx().open();
		}
	}

	public void txCommit() {
		if (tx) {
			gts.tx().commit();
		}
	}

	public void txRollback() {
		if (tx) {
			gts.tx().rollback();
		}
	}

	public Runnable deleteAllDataFunction() {
		return () -> {
			txOpen();
			try {
				//TODO bug, V().drop.iterate() fails on deleting same edges
				gts.clone().V().hasLabel("predictionstructure").drop().iterate();
				txCommit();
				txOpen();
				gts.clone().V().hasLabel("inputstructure").drop().iterate();
				txCommit();
				txOpen();
				gts.clone().V().hasLabel("endpoint").drop().iterate();
			} finally {
				txCommit();
			}
		};
	}
}