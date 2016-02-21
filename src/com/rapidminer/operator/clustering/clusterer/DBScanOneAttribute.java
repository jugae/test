/*
 *  RapidMiner
 *
 *  Copyright (C) 2001-2014 by RapidMiner and the contributors
 *
 *  Complete list of developers available at our web site:
 *
 *       http://rapidminer.com
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package com.rapidminer.operator.clustering.clusterer;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.Tools;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.operator.OperatorCapability;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.clustering.ClusterModel;
import com.rapidminer.operator.learner.CapabilityProvider;
import com.rapidminer.operator.ports.metadata.DistanceMeasurePrecondition;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.math.similarity.DistanceMeasure;
import com.rapidminer.tools.math.similarity.DistanceMeasureHelper;
import com.rapidminer.tools.math.similarity.DistanceMeasures;

/**
 * This operator provides the DBScan cluster algorithm. If no id attribute is present, the operator will create one.
 * @author Julian Gaedecke
 */
public class DBScanOneAttribute extends RMAbstractClusterer implements CapabilityProvider {

	private static final String PARAMETER_EPSILON = "epsilon";

	private static final String PARAMETER_MIN_POINTS= "min_points";

	private DistanceMeasureHelper measureHelper = new DistanceMeasureHelper(this);


	private boolean[] visited; // = new boolean[exampleSet.size()];
	private boolean[] noised;
	int[] clusterAssignments;
	
	public DBScanOneAttribute(OperatorDescription description) {
		super(description);
		
		getExampleSetInputPort().addPrecondition(new DistanceMeasurePrecondition(getExampleSetInputPort(), this));
	}
	
	@Override
	public boolean supportsCapability(OperatorCapability capability) {
		int measureType = DistanceMeasures.MIXED_MEASURES_TYPE;
		try {
			measureType = measureHelper.getSelectedMeasureType();
		} catch (Exception e) {
			
		}
		switch (capability) {
		case BINOMINAL_ATTRIBUTES:
		case POLYNOMINAL_ATTRIBUTES:
			return (measureType == DistanceMeasures.MIXED_MEASURES_TYPE) ||
			(measureType == DistanceMeasures.NOMINAL_MEASURES_TYPE);
		case NUMERICAL_ATTRIBUTES:
			return (measureType == DistanceMeasures.MIXED_MEASURES_TYPE) ||
			(measureType == DistanceMeasures.DIVERGENCES_TYPE) ||
			(measureType == DistanceMeasures.NUMERICAL_MEASURES_TYPE);
		case POLYNOMINAL_LABEL:
		case BINOMINAL_LABEL:
		case NUMERICAL_LABEL:
		case WEIGHTED_EXAMPLES:
		case MISSING_VALUES:
			return true;
		default:
			return false;
		}
	}

	@Override
	public ClusterModel generateClusterModel(ExampleSet exampleSet) throws OperatorException {		
		DistanceMeasure measure = measureHelper.getInitializedMeasure(exampleSet);	
		double epsilon = getParameterAsDouble(PARAMETER_EPSILON);
		int minPoints = getParameterAsInt(PARAMETER_MIN_POINTS);

		// checking and creating ids if necessary
		Tools.checkAndCreateIds(exampleSet);

		// additional checks
		Tools.onlyNonMissingValues(exampleSet, "DBScan");

		// extracting attribute names
		Attributes attributes = exampleSet.getAttributes();
		ArrayList<String> attributeNames = new ArrayList<String>(attributes.size());
		for (Attribute attribute: attributes)
			attributeNames.add(attribute.getName());

//		boolean[] noised = new boolean[exampleSet.size()];
//		int[] clusterAssignments = new int[exampleSet.size()];
		noised = new boolean[exampleSet.size()];
		visited = new boolean[exampleSet.size()];
		clusterAssignments = new int[exampleSet.size()];
		

		int clusterIndex = 1;
		for (int i=0; i<exampleSet.size(); i++) {
			checkForStop();
			if (visited[i]) {
				continue;
			}
			visited[i] = true;
			Queue<Integer> centerNeighbourhood = getNeighbourhood(exampleSet, measure, epsilon, i);
			if (centerNeighbourhood.size() < minPoints) {
				noised[i] = true;
			} else {
				// expanding cluster within density borders
				expandCluster(exampleSet, i, centerNeighbourhood, clusterIndex, epsilon, minPoints);
				// step to next cluster
				clusterIndex++;
			}
		}

		ClusterModel model = new ClusterModel(exampleSet, Math.max(clusterIndex, 1), getParameterAsBoolean(RMAbstractClusterer.PARAMETER_ADD_AS_LABEL), getParameterAsBoolean(RMAbstractClusterer.PARAMETER_REMOVE_UNLABELED));
		model.setClusterAssignments(clusterAssignments, exampleSet);

		if (addsClusterAttribute()) {
			Attribute cluster = AttributeFactory.createAttribute(Attributes.CLUSTER_NAME, Ontology.NOMINAL);
			exampleSet.getExampleTable().addAttribute(cluster);
			exampleSet.getAttributes().setCluster(cluster);
			int i = 0;
			for (Example example: exampleSet) {
				example.setValue(cluster, "cluster_" + clusterAssignments[i]);
				i++;
			}
		}
		return model;
	}
	
	private void expandCluster(ExampleSet exampleSet, int centerIndex, Queue<Integer> neighborPts, int clusterIndex, double eps, int minPtrs) throws OperatorException {
		DistanceMeasure measure = measureHelper.getInitializedMeasure(exampleSet);
		
		// then its center point of a cluster. Assign example to new cluster
		clusterAssignments[centerIndex] = clusterIndex;
		
		while (neighborPts.size() > 0) {
			int currentIndex = neighborPts.poll().intValue();
//		
			// assigning example to current cluster
			clusterAssignments[currentIndex] = clusterIndex;
			visited[currentIndex] = true;
		
			// appending own neighbourhood to queue
			Queue<Integer> neighbourhood = getNeighbourhoodWithoutVisited(exampleSet, measure, eps, minPtrs, currentIndex);
			if (neighbourhood.size() >= minPtrs) {
				// then this neighbor of center is also a center of the cluster
				while (neighbourhood.size() > 0) {
					int neighbourIndex = neighbourhood.poll().intValue();
					if (!visited[neighbourIndex]) {
						if (!noised[neighbourIndex]) {
							// if its not noised, then it might be center of cluster! So append to queue
							neighborPts.add(neighbourIndex);
						} else 
						clusterAssignments[neighbourIndex] = clusterIndex;
						visited[neighbourIndex] = true;
					}
				}
			}
		}
	}

	private LinkedList<Integer> getNeighbourhood(ExampleSet exampleSet, DistanceMeasure measure, double epsilon, int centerExampleIndex) {		
		
		Example centerExample = exampleSet.getExample(centerExampleIndex);
		
		LinkedList<Integer> neighbourhood = new LinkedList<Integer>();
		for (int left=centerExampleIndex-1; left >= 0; left--) {
			Example example = exampleSet.getExample(left);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addFirst(left);
			} else {
				break;
			}
		}
		for (int right=centerExampleIndex; right < exampleSet.size(); right++) {
			Example example = exampleSet.getExample(right);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addLast(right);
			} else {
				break;
			}
		}
		
		return neighbourhood;
	}
	
	private LinkedList<Integer> getNeighbourhoodWithoutVisited(ExampleSet exampleSet, DistanceMeasure measure, double epsilon, int minPtrs, int centerExampleIndex) {		
		
		Example centerExample = exampleSet.getExample(centerExampleIndex);
		
		LinkedList<Integer> neighbourhood = new LinkedList<Integer>();
		for (int left=centerExampleIndex-1; left >= 0; left--) {
			if(visited[left] && neighbourhood.size() >= minPtrs) {
				continue;
			}
				Example example = exampleSet.getExample(left);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addFirst(left);
			} else {
				break;
			}
		}
		
		for (int right=centerExampleIndex; right < exampleSet.size(); right++) {
			if(visited[right] && neighbourhood.size() >= minPtrs) {
				continue;
			}
			Example example = exampleSet.getExample(right);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addLast(right);
			} else {
				break;
			}
		}
		
		return neighbourhood;
	}
	
	private LinkedList<Integer> getNeighbourhoodCheck(Example centerExample, ExampleSet exampleSet, DistanceMeasure measure, double epsilon, int centerExampleIndex) {		
		LinkedList<Integer> oldNeighbourhood = new LinkedList<Integer>();
		int i = 0;
		for (Example example: exampleSet) {
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) 
				oldNeighbourhood.add(i);
			i++;
		}
		
		LinkedList<Integer> neighbourhood = new LinkedList<Integer>();
		for (int left=centerExampleIndex-1; left >= 0; left--) {
			Example example = exampleSet.getExample(left);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addFirst(left);
			} else {
				break;
			}
		}
		
		for (int right=centerExampleIndex; right < exampleSet.size(); right++) {
			Example example = exampleSet.getExample(right);
			double distance = measure.calculateDistance(centerExample, example);
			if (distance < epsilon) {
				neighbourhood.addLast(right);
			} else {
				break;
			}
		}
		
		if (oldNeighbourhood.size() != neighbourhood.size()) {
			System.out.println("default: " + oldNeighbourhood.size() + " / optimized: " + neighbourhood.size());
		} else if (this.listEquals(oldNeighbourhood, neighbourhood) == false) {
			System.out.println("Listen sind nicht identisch!!!");
		}
		
		return neighbourhood;
	}
	
	boolean listEquals(LinkedList<Integer> first, LinkedList<Integer> second) {
		for (int i = 0; i<first.size(); i++) {
			if (!first.get(i).equals(second.get(i))) {
				return false;
			}
		}
		return true;
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = new LinkedList<ParameterType>();
		types.add(new ParameterTypeDouble(PARAMETER_EPSILON, "Specifies the size of neighbourhood.", 0, Double.POSITIVE_INFINITY, 1, false));
		types.add(new ParameterTypeInt(PARAMETER_MIN_POINTS, "The minimal number of points forming a cluster.", 1, Integer.MAX_VALUE, 5, false));

		types.addAll(super.getParameterTypes());

		types.addAll(DistanceMeasures.getParameterTypes(this));

		return types;
	}	
}

