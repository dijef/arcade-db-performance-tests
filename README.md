"# arcade-db-performance-tests" 

To generate test data file run (10k included by default)

	RecordsGenerator
		
	modify RecordsGenerator#recordsNo for desired number of entries
	
	place it inside src/main/resources/ and update PerformanceTest#testDataFilename to it
	
Before running tests

	Download latest version of ArcadeDB and align dependency in pom.xml to match
	
	Unpack ArcadeDB and start it with: server.bat "-Darcadedb.server.plugins=GremlinServer:com.arcadedb.server.gremlin.GremlinServerPlugin"
	Use "root" "password" as credentials.
	If Gremlin error shows. Stop and start again.
	
	Create database: 'test' through ArcadeDB Studio

Running performance test

	Available classes (all with main methods):
	
		ArcadeDbGraphLocalPerformanceTest - local ArcadeGraph
		ArcadeDbTraversalLocalPerformanceTest - local Traversal from ArcadeGraph
		
		ArcadeDbRemoteDatabasePerformanceTest - RemoteDatabase
		
		ArcadeDbGraphRemotePerformanceTest - remote ArcadeGraph
		ArcadeDbTraversalRemotePerformanceTest - remote Gremlin Server Traversal
		
		
		