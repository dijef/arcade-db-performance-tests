package com.pawelmaslej.arcadedbperformancetests;

import java.io.IOException;

import org.slf4j.bridge.SLF4JBridgeHandler;

import com.arcadedb.remote.RemoteDatabase;
import com.pawelmaslej.arcadedbperformancetests.test.PerformanceTest;

import lombok.extern.slf4j.Slf4j;

/**
 * @author Pawel Maslej
 * @since 27 Oct 2023
 */
@Slf4j
public class ArcadeDbRemoteDatabasePerformanceTest {

	public static final String dbAddress = "localhost";
	public static final int dbPort = 2480;
	public static final String dbName = "test";
	public static final String dbUser = "root";
	public static final String dbPass = "password";

	PerformanceTest test;
	RemoteDatabase rd;

	public static final int LIMIT_10K = 10000;
	public static final int LIMIT_100K = 100000;
	public static final int LIMIT_500K = 500000;
	public static final int LIMIT_1M = 1000000;

	public static void main(String [] args) throws Exception {
		ArcadeDbRemoteDatabasePerformanceTest main = new ArcadeDbRemoteDatabasePerformanceTest();
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

		//TODO code no longer compiles, missing methods
//		if (!rd.exists()) {
//			rd.create();
//		} else {
//			rd.drop();
//			rd.create();
//		}

		rd.command("sqlscript", "create vertex type inputstructure if not exists;");
		rd.command("sqlscript", "create vertex type endpoint if not exists; create property endpoint.name if not exists string; create index if not exists on endpoint (name) unique;");
		rd.command("sqlscript", "create vertex type predictionstructure if not exists;");
		rd.command("sqlscript", "create edge type e_endpoint if not exists;");
		rd.command("sqlscript", "create edge type e_predictionstructure if not exists;");
	}

	public void shutdown() throws Exception {
		rd.close();
	}

	public void test() throws IOException {
		test = new PerformanceTest();
		RemoteDatabaseTestCode code = new RemoteDatabaseTestCode(rd, test, false);
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