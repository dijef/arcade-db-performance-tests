package com.pawelmaslej.arcadedbperformancetests;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.arcadedb.gremlin.ArcadeGraph;
import com.arcadedb.remote.RemoteDatabase;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Pawel Maslej
 * @since 27 Oct 2023
 */
@Slf4j
public class ArcadeDbGraphRemotePerformanceTest {

	public static final String dbAddress = "localhost";
	public static final int dbPort = 2480;
	public static final String dbName = "test";
	public static final String dbUser = "root";
	public static final String dbPass = "password";

	PerformanceTest test;
	RemoteDatabase rd;
	ArcadeGraph graph;
	GraphTraversalSource gts;

	public static final int LIMIT_10K = 10000;
	public static final int LIMIT_100K = 100000;
	public static final int LIMIT_500K = 500000;
	public static final int LIMIT_1M = 1000000;

	public static void main(String [] args) throws Exception {
		ArcadeDbGraphRemotePerformanceTest main = new ArcadeDbGraphRemotePerformanceTest();
		try {
			main.configureDB();
			main.test();
		}
		catch (Exception e) {
			log.error(e.getMessage(), e);
		}
		finally {
			main.shutdown();
		}
	}

	public void configureDB() throws Exception {
		SLF4JBridgeHandler.removeHandlersForRootLogger();
		SLF4JBridgeHandler.install();

		rd = new RemoteDatabase("localhost", 2480, "test", "root", "password");
		rd.command("sqlscript", "create vertex type endpoint if not exists; create property endpoint.name if not exists string; create index if not exists on endpoint (name) unique;");

		graph = ArcadeGraph.open(rd);
		gts = graph.traversal();
		graph.tx().onReadWrite(READ_WRITE_BEHAVIOR.MANUAL); // disables auto-start

		// Multi-threading example
		//TODO ArcadeGraphFactory is missing inside dependencies!
//		try (ArcadeGraphFactory pool = ArcadeGraphFactory.withRemote("127.0.0.1", 2480, "mydb", "root", "playwithdata")) {
//			  try( ArcadeGraph graph = pool.get() ){
//			    // DO SOMETHING WITH ARCADE GRAPH
//			  }
//			}
	}

	public void shutdown() throws Exception {
		gts.close();
		graph.close();
		rd.close();
	}

	public void test() throws IOException {
		test = new PerformanceTest();
		TinkerPopTestCode code = new TinkerPopTestCode(graph, gts, test, true);
		test.runTest(
			getClass().getSimpleName(),
			LIMIT_10K,
			code.deleteAllDataFunction(),
			code.createEndpointFunction(),
			code.insertFunction(),
			code.predictFunction(),
			code.queryAndIterateJsonAllInputStructuresFunction(),
			code.queryAndIterateDataAllPredictionStructuresFunction(),
			code.findAllEndpointsFunction(),
			code.findAllPredictionStructuresByEndpointIdFunction(),
			code.printStatsFunction());
	}
}