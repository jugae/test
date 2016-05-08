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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.rapidminer.parameter.ParameterTypeAttribute;
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
public class DBScanWeighted extends RMAbstractClusterer implements CapabilityProvider {

	private static final String PARAMETER_EPSILON = "epsilon";

	private static final String PARAMETER_MIN_POINTS= "min_points";

	public static final String PARAMETER_WEIGHT_ATTRIBUTE_NAME = "weight";
	
	public static final String PARAMETER_FEATURE_ATTRIBUTE_NAME = "feature";
	
	private DistanceMeasureHelper measureHelper = new DistanceMeasureHelper(this);


	private boolean[] visited; // = new boolean[exampleSet.size()];
	private boolean[] noised;
	int[] clusterAssignments;
	
	private String weightAttributeName;
	private String featureAttributeName;
	
	private double eps;
	private int minpts;
	
	private DistanceMeasure measure;
	
	public DBScanWeighted(OperatorDescription description) {
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
			
		measure = measureHelper.getInitializedMeasure(exampleSet);
		minpts = getParameterAsInt(PARAMETER_MIN_POINTS);
		eps = getParameterAsDouble(PARAMETER_EPSILON);
		
		featureAttributeName = getParameterAsString(PARAMETER_FEATURE_ATTRIBUTE_NAME);
		weightAttributeName =  getParameterAsString(PARAMETER_WEIGHT_ATTRIBUTE_NAME);
		
		// checking and creating ids if necessary
		Tools.checkAndCreateIds(exampleSet);

		// additional checks
		Tools.onlyNonMissingValues(exampleSet, "DBScan");

		// extracting attribute names
		Attributes attributes = exampleSet.getAttributes();
		ArrayList<String> attributeNames = new ArrayList<String>(attributes.size());
		for (Attribute attribute: attributes)
			attributeNames.add(attribute.getName());


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
			Object[] list = getNeighbourhood(exampleSet, i);
			Queue<Integer> centerNeighbourhood = (Queue<Integer>)list[0];
			int neighboursCount = (Integer) list[1];
			if (neighboursCount < minpts) {
				noised[i] = true;
			} else {
				// expanding cluster within density borders
				expandCluster(exampleSet, i, centerNeighbourhood, clusterIndex);
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
	
	private void expandCluster(ExampleSet exampleSet, int centerIndex, Queue<Integer> neighborPts, int clusterIndex) throws OperatorException {
		
		// then its center point of a cluster. Assign example to new cluster
		clusterAssignments[centerIndex] = clusterIndex;
		
		while (neighborPts.size() > 0) {
			int currentIndex = neighborPts.poll().intValue();
//		
			// assigning example to current cluster
			clusterAssignments[currentIndex] = clusterIndex;
			visited[currentIndex] = true;
		
			// appending own neighbourhood to queue
			Object[] list = getNeighbourhood(exampleSet, currentIndex);
			Queue<Integer> neighbourhood = (Queue<Integer>)list[0];
			int neighboursCount = (Integer) list[1];
			
			if (neighboursCount >= minpts) {
				// then this neighbor of center is also a center of the cluster
				while (neighbourhood.size() > 0) {
					int neighbourIndex = neighbourhood.poll().intValue();
					if (!visited[neighbourIndex]) {
						if (!noised[neighbourIndex]) {
							// if its not noised, then it might be center of cluster! So append to queue
							neighborPts.add(neighbourIndex);
						}
						
						clusterAssignments[neighbourIndex] = clusterIndex;
						visited[neighbourIndex] = true;
					}
				}
			}
		}
	}

	/**
	 * @param exampleSet
	 * @param measure
	 * @param epsilon
	 * @param centerExampleIndex
	 * @return array with two values. First is the neighbourhood list of indices which are in epsilon. The Second value will be the minpts.
	 */
	private Object[] getNeighbourhood(ExampleSet exampleSet, int centerExampleIndex) {		
		
		Example centerExample = exampleSet.getExample(centerExampleIndex);
		
		int points = 0;
		LinkedList<Integer> neighbourhood = new LinkedList<Integer>();
		for (int left=centerExampleIndex-1; left >= 0; left--) {
			Example example = exampleSet.getExample(left);
			double distance = calculateDistance(centerExample, example);
			if (distance < eps) {
				neighbourhood.addLast(left);
				points += example.getNumericalValue(example.getAttributes().get(weightAttributeName));
			} else {
				break;
			}
		}
		for (int right=centerExampleIndex; right < exampleSet.size(); right++) {
			Example example = exampleSet.getExample(right);
			double distance = calculateDistance(centerExample, example);
			if (distance < eps) {
				neighbourhood.addLast(right);
				points += example.getNumericalValue(example.getAttributes().get(weightAttributeName));
			} else {
				break;
			}
		}
		
		Object [] objects = { neighbourhood, points };
		
		return objects;
	}
	
	private Object[] getNeighbourhoodWithoutVisited(ExampleSet exampleSet, int centerExampleIndex) {		
		
		Example centerExample = exampleSet.getExample(centerExampleIndex);
		
		int points = 0;
		LinkedList<Integer> neighbourhood = new LinkedList<Integer>();
		
		for (int left=centerExampleIndex-1; left >= 0; left--) {
			Example example = exampleSet.getExample(left);
			double distance = calculateDistance(centerExample, example);
			if (distance < eps) {
				if(visited[left] && points >= minpts) {
					continue; // test break; 
				}
				neighbourhood.addLast(left);
				points += example.getNumericalValue(example.getAttributes().get(weightAttributeName));
			} else {
				break;
			}
		}
		
		for (int right=centerExampleIndex; right < exampleSet.size(); right++) {
			Example example = exampleSet.getExample(right);
			double distance = calculateDistance(centerExample, example);
			if (distance < eps) {
				if(visited[right] && points >= minpts) {
					continue;
				}
				
				neighbourhood.addLast(right);
				points += example.getNumericalValue(example.getAttributes().get(weightAttributeName));
			} else {
				break;
			}
		}
	
		Object [] objects = { neighbourhood, points };
		
		return objects;
	}

	
	private double calculateDistance(Example first, Example second) {
		Attribute firstAttribute = first.getAttributes().get(featureAttributeName);
		double[] dblFirst = { first.getNumericalValue(firstAttribute) }; 
		Attribute secondAttribute = second.getAttributes().get(featureAttributeName);
		double[] dblSecond = { second.getNumericalValue(secondAttribute) };
		
		return measure.calculateDistance(dblFirst, dblSecond);
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

		types.add(new ParameterTypeAttribute(PARAMETER_WEIGHT_ATTRIBUTE_NAME, "The name of the nominal attribute to which values should be added.", getExampleSetInputPort(), false));
		types.add(new ParameterTypeAttribute(PARAMETER_FEATURE_ATTRIBUTE_NAME, "The name of the nominal attribute to which values should be added.", getExampleSetInputPort(), false));
		
		types.addAll(super.getParameterTypes());

		types.addAll(DistanceMeasures.getParameterTypes(this));

		return types;
	}	
}

