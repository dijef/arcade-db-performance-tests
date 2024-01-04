package com.pawelmaslej.arcadedbperformancetests.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import com.google.common.collect.Lists;
import com.pawelmaslej.arcadedbperformancetests.utils.Utils;

/**
 * @author Pawel Maslej
 * @since 27 Oct 2023
 */
public class PerformanceTest {

	static String testDataFilename = "/data_10000";
	static int batchSize = 10000;
	static URL sdfUrl = null;
	static Set<Stage> stages = new HashSet<PerformanceTest.Stage>();

	AtomicLong operationsTime = new AtomicLong();

	List<String> endpoints = Arrays.asList("Carcinogenicity", "Chromosome Damage", "Hepatoxicity", "Mutagenicity", "Skin Iritation", "Skin Sensitisation", "Teratogenicity");
	List<List<String>> alerts = Arrays.asList(Arrays.asList("Alert 1"), Arrays.asList("Alert 1", "Alert 2"), Arrays.asList("Alert 2", "Alert 3"), Arrays.asList("Alert 4"));

	public enum Stage {
		DELETE,
		CREATE_ENDPOINT,
		INSERT,
		GENERATE_PREDICTION,
		QUERY,
		FIND,
		ALL
	}

	static {
		stages.add(Stage.ALL);
	}

	public static void configureStages(Stage ... stages) {
		PerformanceTest.stages.clear();
		for (Stage stage : stages) {
			PerformanceTest.stages.add(stage);
		}
	}

	public static Set<Stage> getStages() {
		return stages;
	}

	public void runTest(
			String name,
			int limit,
			Runnable deleteAllDataFunction,
			Function<String, String> createEndpointFunction,
			Function<List<String>, List<String>> insertFunction,
			Function<List<PredictionStructureInputData>, List<String>> generatePredictionFunction,
			Supplier<List<StructureData>> queryAndIterateJsonAllInputStructuresFunction,
			Supplier<Integer> queryAndIterateDataAllPredictionStructuresFunction,
			Supplier<List<EndpointData>> findAllEndpointsFunction,
			Function<String, List<String>> findAllPredictionStructuresByEndpointIdFunction,
			Runnable printStats)
			throws IOException {
		System.out.println(name);
		System.out.println("Running performance test for %s structures".formatted(limit));

		if (stages.contains(Stage.ALL) || stages.contains(Stage.DELETE)) {
			System.out.println("Deleting all data");
			deleteAllDataFunction.run();
		}

		List<String> endpointIds;
		if (stages.contains(Stage.ALL) || stages.contains(Stage.CREATE_ENDPOINT)) {
			endpointIds = createEndpoints(createEndpointFunction);
			System.out.println("Created %s endpoints in %s requests %s ms".formatted(endpointIds.size(), endpointIds.size(), operationsTimeInMs()));
			System.out.println("Average create endpoint time %s ns".formatted(operationsTimeInNano() / endpointIds.size()));
			reset();
		} else {
			endpointIds = findAllEndpoints(findAllEndpointsFunction).stream().map(EndpointData::getId).toList();
			reset();
		}

		var predictionInputs = generatePredictionInputs(limit, endpointIds);
		List<String> structureIds;
		if (stages.contains(Stage.ALL) || stages.contains(Stage.INSERT)) {
			var records = readRecords();
			structureIds = insertStructures(records, limit, insertFunction);
			System.out.println("Insert input structures time: %s ms".formatted(operationsTimeInMs()));
			System.out.println("Average time per structure insert: %s ns".formatted(operationsTimeInNano()/limit));
			reset();
		} else {
			structureIds = queryAndIterateJsonAllInputStructures(queryAndIterateJsonAllInputStructuresFunction).stream()
				.map(StructureData::id).toList();
			reset();
		}

		if (stages.contains(Stage.ALL) || stages.contains(Stage.GENERATE_PREDICTION)) {
			IntStream.range(0, structureIds.size()).forEach(i -> predictionInputs.get(i).setInputStructureId(structureIds.get(i)));
			var predictionStructureIds = generatePredictionsForInputStructures(predictionInputs, generatePredictionFunction);
			System.out.println("Generate predictions time: %s ms".formatted(operationsTimeInMs()));
			System.out.println("Average time per prediction insert: %s ns".formatted(operationsTimeInNano()/limit));
			reset();
		}

		if (stages.contains(Stage.ALL) || stages.contains(Stage.QUERY)) {
			var inputStructuresRetrievedCount = queryAndIterateJsonAllInputStructures(queryAndIterateJsonAllInputStructuresFunction).size();
			System.out.println("Retrieved all input structures (%s) in time %s ms".formatted(inputStructuresRetrievedCount, operationsTimeInMs()));
			reset();

			var predictionStructuresRetrievedCount = queryAndIterateDataAllPredictionStructures(queryAndIterateDataAllPredictionStructuresFunction);
			System.out.println("Retrieved all prediction structures with related records (%s) in time %s ms".formatted(predictionStructuresRetrievedCount, operationsTimeInMs()));
			reset();
		}

		if (stages.contains(Stage.ALL) || stages.contains(Stage.FIND)) {
			var endpoints = findAllEndpoints(findAllEndpointsFunction);
			System.out.println("Search all endpoints (%s) %s ms".formatted(endpoints.size(), operationsTimeInMs()));
			reset();

			var predictionStructuresByEndpoint = findAllPredictionStructuresByEndpointId(findAllPredictionStructuresByEndpointIdFunction, endpoints.get(0).getId());
			System.out.println("Found %s prediction structures by endpoint id in %s ms".formatted(predictionStructuresByEndpoint.size(), operationsTimeInMs()));
			reset();
		}

		printStats.run();
	}

	private List<String> createEndpoints(Function<String, String> createEndpointFunction) {
		long start = System.nanoTime();
		var endpointIds = new ArrayList<String>(endpoints.size());
		for (var endpointName : endpoints) {
			var value = createEndpointFunction.apply(endpointName);
			endpointIds.add(value);
		}
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return endpointIds;
	}

	public List<RecordData> readRecords() throws IOException {
		List<RecordData> records = new LinkedList<>();
		try (var is = PerformanceTest.class.getResourceAsStream(testDataFilename)) {
			var br = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = br.readLine()) != null) {
				records.add(new RecordData(line));
			}
		}
		return records;
	}

	private List<String> insertStructures(List<RecordData> records, int recordsSize, Function<List<String>, List<String>> l) {
		List<List<Integer>> partitions = Lists.partition(IntStream.range(0, recordsSize).mapToObj(i -> Integer.valueOf(i)).toList(), batchSize);
		List<String> allIds = new ArrayList<String>(recordsSize);
		for (var list : partitions) {
			var ids = insertStructuresForPartition(records, list, l);
			allIds.addAll(ids);
		}
		return allIds;
	}

	private List<String> insertStructuresForPartition(List<RecordData> records, List<Integer> partition, Function<List<String>, List<String>> l) {
		var structuresJson = partition.stream()
			.map(i -> {
				var r = records.get(i.intValue());
				return Utils.toBase64(r.getJson());
			}).toList();
		long start = System.nanoTime();
		var ids = l.apply(structuresJson);
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end - start));
		return ids;
	}

	private List<String> generatePredictionsForInputStructures(List<PredictionStructureInputData> predictionStructureDataInputs, Function<List<PredictionStructureInputData>, List<String>> l) {
		List<List<PredictionStructureInputData>> partitions = Lists.partition(IntStream.range(0, predictionStructureDataInputs.size()).mapToObj(i -> predictionStructureDataInputs.get(i)).toList(), batchSize);
		List<String> allIds = new ArrayList<String>(predictionStructureDataInputs.size());
		for (var list : partitions) {
			var computedIds = generatePredictionsForInputStructuresForPartition(list, l);
			allIds.addAll(computedIds);
		}
		return allIds;
	}

	private List<String> generatePredictionsForInputStructuresForPartition(List<PredictionStructureInputData> predictionStructureDataInputs, Function<List<PredictionStructureInputData>, List<String>> l) {
		long start = System.nanoTime();
		var computedIds = l.apply(predictionStructureDataInputs);
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return computedIds;
	}

	private AtomicLong getOperationsTime() {
		return operationsTime;
	}

	private void reset() {
		operationsTime.set(0);
	}

	private long operationsTimeInNano() {
		return operationsTime.get();
	}

	private long operationsTimeInMs() {
		return operationsTime.get() / 1000000;
	}

	private long operationsTimeInS() {
		return operationsTime.get() / 1000000 / 1000;
	}

	private List<PredictionStructureInputData> generatePredictionInputs(int size, List<String> endpointIds) {
		var endpointsIdsIter = endpointIds.iterator();
		var alertsIter = alerts.iterator();
		List<PredictionStructureInputData> inputs = new ArrayList<>(size);
		for (int i=0; i<size; i++) {
			if (!endpointsIdsIter.hasNext()) {
				endpointsIdsIter = endpointIds.iterator();
			}
			if (!alertsIter.hasNext()) {
				alertsIter = alerts.iterator();
			}
			inputs.add(new PredictionStructureInputData(endpointsIdsIter.next(), alertsIter.next()));
		}
		return inputs;
	}

	public void printStats(int structuresCount, int predictionStructuresCount, int endpointsCount) {
		System.out.println("No of structures %s. No of prediction structures %s. No of endpoints %s.".formatted(structuresCount, predictionStructuresCount, endpointsCount));
	}

	private List<StructureData> queryAndIterateJsonAllInputStructures(Supplier<List<StructureData>> queryAndIterateJsonAllInputStructuresFunction) {
		long start = System.nanoTime();
		var value = queryAndIterateJsonAllInputStructuresFunction.get();
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return value;
	}

	private int queryAndIterateDataAllPredictionStructures(Supplier<Integer> queryAndIterateDataAllPredictionStructuresFunction) {
		long start = System.nanoTime();
		var value = queryAndIterateDataAllPredictionStructuresFunction.get().intValue();
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return value;
	}

	private List<EndpointData> findAllEndpoints(Supplier<List<EndpointData>> findAllEndpointsFunction) {
		long start = System.nanoTime();
		var value = findAllEndpointsFunction.get();
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return value;
	}

	private List<String> findAllPredictionStructuresByEndpointId(Function<String, List<String>> findAllPredictionStructuresByEndpointIdFunction, String id) {
		long start = System.nanoTime();
		var value = findAllPredictionStructuresByEndpointIdFunction.apply(id);
		long end = System.nanoTime();
		operationsTime.set(operationsTime.get() + (end-start));
		return value;
	}
}