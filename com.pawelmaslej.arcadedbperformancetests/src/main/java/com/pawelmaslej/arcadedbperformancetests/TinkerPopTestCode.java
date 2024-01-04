package com.pawelmaslej.arcadedbperformancetests;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
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
public class TinkerPopTestCode {

	Graph graph;
	GraphTraversalSource gts;
	PerformanceTest test;
	boolean tx;

	public TinkerPopTestCode(Graph graph,
		GraphTraversalSource gts,
		PerformanceTest test,
		boolean tx) {
		this.graph = graph;
		this.gts = gts;
		this.test = test;
		this.tx = tx;
	}

	public Function<List<String>, List<String>> insertFunction() {
		return (jsonList) -> {
			List<Vertex> vertices = new ArrayList<>(jsonList.size());
			txOpen();
			try {
				for (var json : jsonList) {
					var v = graph.addVertex("inputstructure");
					v.property("json", json);
					vertices.add(v);
				}
			}
			finally {
				txCommit();
			}
			return vertices.stream().map(v -> v.id().toString()).toList();
		};
	}

	public Function<List<PredictionStructureInputData>, List<String>> predictFunction() {
		return (inputDataList) -> {
			List<Vertex> vertices = new ArrayList<>(inputDataList.size());
			for (var inputData : inputDataList) {
				txOpen();
				try {
					var isv = graph.vertices(inputData.getInputStructureId()).next();
					var inputStructureJson = isv.value("json");
					var psv = graph.addVertex("predictionstructure");
					psv.property("json", inputStructureJson);
					var ev = graph.vertices(inputData.getEndpointId()).next();
					psv.addEdge("e_endpoint", ev);
					isv.addEdge("e_predictionstructure", psv);
					psv.property("alerts", Utils.listToString(inputData.getAlerts()));

					vertices.add(psv);
				}
				finally {
					txCommit();
				}
			}
			return vertices.stream().map(v -> v.id().toString()).toList();
		};
	}

	public Function<String, String> createEndpointFunction() {
		return (String endpointName) -> {
			txOpen();
			var	v = graph.addVertex("endpoint");
			v.property("name", endpointName);
			txCommit();
			return v.id().toString();
		};
	}

	public Supplier<List<StructureData>> queryAndIterateJsonAllInputStructuresFunction() {
		return () -> {
			txOpen();
			try {
				var inputStructuresV = gts.clone()
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
					var edges = v.edges(Direction.OUT, "e_endpoint");
					while (edges.hasNext()) {
						edgesCount++;
						var ev = edges.next().inVertex();
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
				var ev = graph.vertices(id).next();
				var edgesIter = ev.edges(Direction.IN, "e_endpoint");
				List<String> ids = new LinkedList<String>();
				while (edgesIter.hasNext()) {
					var psv = edgesIter.next().outVertex();
					if (psv.id().toString().equals(ev.id().toString())) {
						throw new RuntimeException("Wrong vertex");
					}
					ids.add(psv.id().toString());
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
			if (graph.tx().isOpen()) {
				throw new RuntimeException("Transaction open");
			}
			graph.tx().open();
		}
	}

	public void txCommit() {
		if (tx) {
			graph.tx().commit();
		}
	}

	public void txRollback() {
		if (tx) {
			graph.tx().rollback();
		}
	}

	public Runnable deleteAllDataFunction() {
		return () -> {
			txOpen();
			Iterator<Vertex> iter = null;
			try {
				iter = graph.vertices();
			}
			finally {
				txCommit();
			}
			while (iter.hasNext()) {
				var v = iter.next();
				txOpen();
				try {
					v.remove();
				}
				finally {
					txCommit();
				}
			}
		};
	}
}