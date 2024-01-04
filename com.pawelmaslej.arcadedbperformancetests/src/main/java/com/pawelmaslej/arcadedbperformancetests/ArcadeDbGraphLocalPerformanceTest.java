package com.pawelmaslej.arcadedbperformancetests;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Transaction.READ_WRITE_BEHAVIOR;
import org.slf4j.bridge.SLF4JBridgeHandler;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.gremlin.ArcadeGraph;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Pawel Maslej
 * @since 27 Oct 2023
 */
@Slf4j
public class ArcadeDbGraphLocalPerformanceTest {

	public static final String dbAddress = Paths.get("").toAbsolutePath().toString() + "/database";

	PerformanceTest test;
	DatabaseFactory factory;
	Database db;
	ArcadeGraph graph;
	GraphTraversalSource gts;

	public static final int LIMIT_10K = 10000;
	public static final int LIMIT_100K = 100000;
	public static final int LIMIT_500K = 500000;
	public static final int LIMIT_1M = 1000000;

	public static void main(String [] args) throws Exception {
		ArcadeDbGraphLocalPerformanceTest main = new ArcadeDbGraphLocalPerformanceTest();
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

		factory = new DatabaseFactory(dbAddress);
		factory.setAutoTransaction(false);
		db = getOrCreateDatabase(factory);
		db.command("sqlscript", "create vertex type endpoint if not exists; create property endpoint.name if not exists string; create index if not exists on endpoint (name) unique;");
		graph = ArcadeGraph.open(db);
		graph.tx().onReadWrite(READ_WRITE_BEHAVIOR.MANUAL); // disables auto-start
		gts = graph.traversal();
	}

	public void shutdown() throws Exception {
		gts.close();
		graph.close();
		factory.close();
	}

	public void test() throws IOException {
		test = new PerformanceTest();
		TinkerPopTestCode tinkerCode = new TinkerPopTestCode(graph, gts, test, true);
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

	private static Database getOrCreateDatabase(DatabaseFactory factory) {
		Database db;
		if (!factory.exists()) {
			db = factory.create();
		}
		else {
			db = factory.open();
		}
		db.setAutoTransaction(false);
		return db;
	}
}