package com.pawelmaslej.arcadedbperformancetests.test;

import java.util.List;

import lombok.Getter;
import lombok.Setter;

/**
 * @author Pawel Maslej
 * @since 30 Oct 2023
 */
@Getter
@Setter
public class PredictionStructureInputData {

	String inputStructureId;
	String endpointId;
	List<String> alerts;

	public PredictionStructureInputData(String endpointId, List<String> alerts) {
		this.endpointId = endpointId;
		this.alerts = alerts;
	}
}