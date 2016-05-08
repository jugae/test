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

package com.rapidminer.operator.preprocessing.discretization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedSet;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DoubleArrayDataRow;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.annotation.ResourceConsumptionEstimator;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.preprocessing.AbstractDataProcessing;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeAttribute;
import com.rapidminer.parameter.ParameterTypeCategory;
import com.rapidminer.parameter.ParameterTypeDouble;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.ParameterTypeList;
import com.rapidminer.parameter.ParameterTypeString;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.OperatorResourceConsumptionHandler;
import com.rapidminer.tools.container.Tupel;

/**
 * This operator discretizes a numerical attribute to either a nominal or an ordinal attribute. The numerical values are mapped to the classes according to the thresholds specified by the user. The user can define the classes by specifying the upper
 * limits of each class. The lower limit of the next class is automatically specified as the upper limit of the previous one. A parameter defines to which adjacent class values that are equal to the given limits should be mapped. If the upper limit
 * in the last list entry is not equal to Infinity, an additional class which is automatically named is added. If a '?' is given as class value the according numerical values are mapped to unknown values in the resulting attribute.
 * 
 * @author Julian Gaedecke
 */
public class UserBasedDiscretizationDicts extends AbstractDataProcessing {

	public static final String PARAMETER_DISCRETIZE_ATTRIBUTE_NAME = "discretize_attribute";
	public static final String PARAMETER_DISCRETIZING_ATTRIBUTE_NAME = "discretizing_attribute";
	public static final String PARAMETER_BIN_WIDTH = "bin_width";
	
	/** The parameter name for &quot;Attribute type of the discretized attribute.&quot; */
	public static final String PARAMETER_ATTRIBUTE_TYPE = "attribute_type";

	/** The parameter name for the upper limit. */
	public static final String PARAMETER_UPPER_LIMIT = "upper_limit";

	/** The parameter name for &quot;Defines the classes and the upper limits of each class.&quot; */
	public static final String PARAMETER_RANGE_NAMES = "classes";

	private static final String PARAMETER_CLASS_NAME = "class_names";

	public static final String[] attributeTypeStrings = { "nominal", "ordinal" };

	public static final int ATTRIBUTE_TYPE_NOMINAL = 0;

	public static final int ATTRIBUTE_TYPE_ORDINAL = 1;

	private final InputPort exampleSetInput2 = getInputPorts().createPort("example set input 2");

	private HashMap<Double, Integer> resultMap;
	
	public UserBasedDiscretizationDicts(OperatorDescription description) {
		super(description);
	}

	@Override
	public ExampleSet apply(ExampleSet exampleSet) throws OperatorException {
		
		HashMap<String, SortedSet<Tupel<Double, String>>> ranges = new HashMap<String, SortedSet<Tupel<Double, String>>>();
		ExampleSet discSet = exampleSetInput2.getData(ExampleSet.class);
		String discretizeAttributeName = getParameterAsString(PARAMETER_DISCRETIZE_ATTRIBUTE_NAME);
		Double binWidth =  getParameterAsDouble(PARAMETER_BIN_WIDTH);
		
		Attribute exAttr = exampleSet.getAttributes().get(getParameterAsString(PARAMETER_DISCRETIZE_ATTRIBUTE_NAME));
		
		// create dict with derivation + count
		resultMap  = new HashMap<Double, Integer>();
		
		// loop over the example set 1
		for (Example ex : exampleSet) {
			
			ArrayList<Double> values = new ArrayList<Double>();
			
			for (Attribute a : discSet.getAttributes()) {
				// call method which handles both example sets + current attribute
				Double discVal = discretize(discSet, a, ex, exAttr);
				values.add(discVal);
			}
			
			
			Double stdDerivation = calcStdDerivation(values);
			this.putDerivationToResultMap(stdDerivation,binWidth);
			
		}
		// build new exampleSet on std dev and counts
		ExampleSet resultSet =  createResultSet();
		
		return resultSet;
	}

	private void putDerivationToResultMap(Double stddev, Double binWidth) {
		Double binnedStdDev = (double) (((int)(stddev / binWidth)) * binWidth);
		if (!resultMap.containsKey(binnedStdDev)) {
			resultMap.put(binnedStdDev, 1);
		} else {
			resultMap.put(binnedStdDev, 1+resultMap.get(binnedStdDev));
		}
	}
	
	private ExampleSet createResultSet() {

        // create data table
        List<Attribute> resultAttributes = new LinkedList<Attribute>();
        Attribute resultGroupAttribute = AttributeFactory.createAttribute("stddev", Ontology.NUMERICAL);
        resultAttributes.add(resultGroupAttribute);
        Attribute resulCountAttribute = AttributeFactory.createAttribute("count", Ontology.NUMERICAL);
        resultAttributes.add(resulCountAttribute);
        
        for (Attribute attribute : resultAttributes) {
            attribute.setConstruction(attribute.getName());
        }
        MemoryExampleTable resultTable = new MemoryExampleTable(resultAttributes);

        
        // fill data table
        
        
        for (Double key : resultMap.keySet()) {
			Integer value = resultMap.get(key);

			double[] data = new double[2];
	        data[0] = key;
	        data[1] = value;
	        resultTable.addDataRow(new DoubleArrayDataRow(data));
		}
        
        ExampleSet resultSet = resultTable.createExampleSet();
        return resultSet;
	}
	
	private Double calcStdDerivation(ArrayList<Double> values) {

		Double mean = 0.0;
		for (Double value : values) {
			mean += value;
		}
		mean /= values.size();
		
		Double var = 0.0;
		for (Double value : values) {
			var += Math.pow(value - mean, 2);
		}
		var /= values.size();
		
		return Math.sqrt(var);
		
	}

	public Double discretize(ExampleSet discSet, Attribute discAttr, Example example, Attribute exAttr) {
		
		Double exampleVal = example.getNumericalValue(exAttr);
		
		for (Example discEx : discSet) {
			double discVal = discEx.getNumericalValue(discAttr);
			if (exampleVal <= discVal) {
				return discVal;
			}
		}
		
		// If no interval found, then it is behind the last. The last interval is connected to the first interval
		return discSet.getExample(0).getNumericalValue(discAttr);
		
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = super.getParameterTypes();

		ParameterType type = new ParameterTypeCategory(PARAMETER_ATTRIBUTE_TYPE, "Attribute type of the discretized attribute.", attributeTypeStrings, ATTRIBUTE_TYPE_NOMINAL);
		type.setExpert(false);
		types.add(type);

		types.add(new ParameterTypeAttribute(PARAMETER_DISCRETIZE_ATTRIBUTE_NAME, "The name of the attribute from second example set which defines the bounds of discritization.", exampleSetInput2, false));
		types.add(new ParameterTypeDouble(PARAMETER_BIN_WIDTH, "bin width.", 0, Double.MAX_VALUE));
		types.add(new ParameterTypeAttribute(PARAMETER_DISCRETIZING_ATTRIBUTE_NAME, "The name of attribute from example set.", getExampleSetInputPort(), false));
		
		ParameterType classType = new ParameterTypeString(PARAMETER_CLASS_NAME, "The name of this range.");
		ParameterType threshold = new ParameterTypeDouble(PARAMETER_UPPER_LIMIT, "The upper limit.", Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
		List<String[]> defaultList = new LinkedList<String[]>();

		defaultList.add(new String[] { "first", Double.NEGATIVE_INFINITY + "" });
		defaultList.add(new String[] { "last", Double.POSITIVE_INFINITY + "" });

		type = new ParameterTypeList(PARAMETER_RANGE_NAMES, "Defines the classes and the upper limits of each class.", classType, threshold, defaultList);
		type.setExpert(false);
		types.add(type);

		return types;
	}

	@Override
	public ResourceConsumptionEstimator getResourceConsumptionEstimator() {
		return OperatorResourceConsumptionHandler.getResourceConsumptionEstimator(getInputPort(), UserBasedDiscretizationDicts.class, null);
	}

}
