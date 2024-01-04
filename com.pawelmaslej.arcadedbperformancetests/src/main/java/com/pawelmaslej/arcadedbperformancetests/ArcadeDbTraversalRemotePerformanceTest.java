package com.pawelmaslej.arcadedbperformancetests;

import java.io.IOException;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.arcadedb.remote.RemoteDatabase;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Pawel Maslej
 * @since 27 Oct 2023
 */
@Slf4j
public class ArcadeDbTraversalRemotePerformanceTest {

	public static final String dbAddress = "localhost";
	public static final int dbPort = 2480;
	public static final String dbName = "test1";
	public static final String dbUser = "root";
	public static final String dbPass = "password";

	PerformanceTest test;
	Cluster cluster;
	DriverRemoteConnection drc;
	GraphTraversalSource gts;

	public static final int LIMIT_10K = 10000;
	public static final int LIMIT_100K = 100000;
	public static final int LIMIT_500K = 500000;
	public static final int LIMIT_1M = 1000000;

	public static void main(String [] args) throws Exception {
		ArcadeDbTraversalRemotePerformanceTest main = new ArcadeDbTraversalRemotePerformanceTest();
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

		var rd = new RemoteDatabase("localhost", 2480, "graph", "root", "password");
		rd.command("sqlscript", "create vertex type endpoint if not exists; create property endpoint.name if not exists string; create index if not exists on endpoint (name) unique;");
		rd.close();

		cluster = Cluster.build()
	        .port(8182)
	        .addContactPoint("localhost")
	        .credentials("root", "password")
	        .create();

		drc = DriverRemoteConnection.using(cluster);
		gts = new GraphTraversalSource(drc);
	}

	public void shutdown() throws Exception {
		gts.close();
		drc.close();
		cluster.close();
	}

	public void test() throws IOException {
		test = new PerformanceTest();
		TraversalTinkerPopTestCode tinkerCode = new TraversalTinkerPopTestCode(gts, test, false);
		test.runTest(
			getClass().getSimpleName(),
			LIMIT_10K,
			tinkerCode.deleteAllDataFunction(),
			tinkerCode.createEndpointFunction(),
			tinkerCode.insertFunction(),
			tinkerCode.predictFunction(),
			tinkerCode.queryAndIterateJsonAllInputStructuresFunction(),
			tinkerCode.queryAndIterateDataAllPredictionStructuresFunction(),
			tinkerCode.findAllEndpointsFunction(),
			tinkerCode.findAllPredictionStructuresByEndpointIdFunction(),
			tinkerCode.printStatsFunction());
	}
}