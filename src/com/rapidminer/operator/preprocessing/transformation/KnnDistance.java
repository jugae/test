/**
 * RapidMiner
 *
 * Copyright (C) 2001-2015 by RapidMiner and the contributors
 *
 * Complete list of developers available at our web site:
 *
 *      https://rapidminer.com
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see http://www.gnu.org/licenses/.
 */
package com.rapidminer.operator.preprocessing.transformation;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.rapidminer.example.Attribute;
import com.rapidminer.example.Attributes;
import com.rapidminer.example.Example;
import com.rapidminer.example.ExampleSet;
import com.rapidminer.example.Tools;
import com.rapidminer.example.set.SimpleExampleSet;
import com.rapidminer.example.table.AttributeFactory;
import com.rapidminer.example.table.DataRow;
import com.rapidminer.example.table.DataRowFactory;
import com.rapidminer.example.table.DataRowReader;
import com.rapidminer.example.table.ExampleTable;
import com.rapidminer.example.table.MemoryExampleTable;
import com.rapidminer.operator.Operator;
import com.rapidminer.operator.OperatorDescription;
import com.rapidminer.operator.OperatorException;
import com.rapidminer.operator.ports.InputPort;
import com.rapidminer.operator.ports.OutputPort;
import com.rapidminer.operator.ports.metadata.AttributeMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetMetaData;
import com.rapidminer.operator.ports.metadata.ExampleSetPassThroughRule;
import com.rapidminer.operator.ports.metadata.ExampleSetPrecondition;
import com.rapidminer.operator.ports.metadata.SetRelation;
import com.rapidminer.parameter.ParameterType;
import com.rapidminer.parameter.ParameterTypeInt;
import com.rapidminer.parameter.UndefinedParameterError;
import com.rapidminer.tools.Ontology;
import com.rapidminer.tools.math.Complex;
import com.rapidminer.tools.math.FastFourierTransform;
import com.rapidminer.tools.math.Peak;
import com.rapidminer.tools.math.SpectrumFilter;
import com.rapidminer.tools.math.WindowFunction;
import com.rapidminer.tools.math.similarity.DistanceMeasure;
import com.rapidminer.tools.math.similarity.DistanceMeasureHelper;
import com.rapidminer.tools.math.similarity.DistanceMeasures;


/**
 * This is the KnnDistance tutorial operator.
 *
 * @author Julian Gaedecke
 */
public class KnnDistance extends Operator {

	private InputPort exampleSetInput = getInputPorts().createPort("example set");
	private OutputPort exampleSetOutput = getOutputPorts().createPort("example set");

	private static final String PARAMETER_K = "k";

	private DistanceMeasureHelper measureHelper = new DistanceMeasureHelper(this);
	
	/**
	 * The default constructor needed in exactly this signature
	 *
	 * @param description
	 *            the operator description
	 */
	public KnnDistance(OperatorDescription description) {
		super(description);

		exampleSetInput.addPrecondition(new ExampleSetPrecondition(exampleSetInput, new String[] { "relative time" },
				Ontology.ATTRIBUTE_VALUE));

		getTransformer().addRule(new ExampleSetPassThroughRule(exampleSetInput, exampleSetOutput, SetRelation.EQUAL) {

			@Override
			public ExampleSetMetaData modifyExampleSet(ExampleSetMetaData metaData) throws UndefinedParameterError {
				AttributeMetaData timeAMD = metaData.getAttributeByName("relative time");
				if (timeAMD != null) {
					timeAMD.setType(Ontology.DATE_TIME);
					timeAMD.setName("date(" + timeAMD.getName() + ")");
					timeAMD.setValueSetRelation(SetRelation.UNKNOWN);
				}
				return metaData;
			}
		});
	}

	
	@Override
	public void doWork() throws OperatorException {
		ExampleSet exampleSet = exampleSetInput.getData(ExampleSet.class);
		DistanceMeasure measure = measureHelper.getInitializedMeasure(exampleSet);
		int k = getParameterAsInt(PARAMETER_K);
		
		
		// extracting attribute names
		Attributes attributes = exampleSet.getAttributes();
		ArrayList<String> attributeNames = new ArrayList<String>(attributes.size());
		for (Attribute attribute: attributes)
			attributeNames.add(attribute.getName());
		Attribute sourceAttribute = attributes.get(attributeNames.get(0));
		String newName = "k-dist (" + sourceAttribute.getName() + ")";
		
		// create new example table
		int numberOfNewExamples = exampleSet.size();
		ExampleTable exampleTable = new MemoryExampleTable(new LinkedList<Attribute>(), new DataRowFactory(DataRowFactory.TYPE_DOUBLE_ARRAY, '.'), numberOfNewExamples);

		// create frequency attribute (for frequency)
		Attribute srcAttribute = AttributeFactory.createAttribute(sourceAttribute.getName(), Ontology.REAL);
		Attribute distAttribute= AttributeFactory.createAttribute(newName, Ontology.REAL);
		exampleTable.addAttribute(srcAttribute);
		exampleTable.addAttribute(distAttribute);
		DataRowReader drr = exampleTable.getDataRowReader();
		int i = 0;
		int odd = (k%2==0) ? 0 : 1;
		while (drr.hasNext()) {
			DataRow dataRow = drr.next();
			Example currentIndexValue = exampleSet.getExample(i);
			dataRow.set(srcAttribute, currentIndexValue.getValue(sourceAttribute));

			int begin = i-k/2;
			int end = i+(k/2)+odd;
			if (i==0)
				System.out.println("begin: "+ begin + " and end: " + end);
			if (begin>=0 && end<exampleTable.size()) {
				double overall = 0.0;
				for (int j=begin ; j<=end; j++) {
					Example distanceComparingValue = exampleSet.getExample(j);
					double distance = measure.calculateDistance(currentIndexValue, distanceComparingValue);
					if (distance > overall) {
						overall = distance;
					}
				}

				dataRow.set(distAttribute, overall);
			} else {
				dataRow.set(distAttribute, 0);
			}
			
			
			i++;
		}

		ExampleSet resultSet = new SimpleExampleSet(exampleTable);

		exampleSetOutput.deliver(resultSet);
	}

	@Override
	public List<ParameterType> getParameterTypes() {
		List<ParameterType> types = new LinkedList<ParameterType>();
		types.add(new ParameterTypeInt(PARAMETER_K, "Specifies the count of neighbourhood.", 1, Integer.MAX_VALUE, 5, false));

		types.addAll(super.getParameterTypes());

		types.addAll(DistanceMeasures.getParameterTypes(this));

		return types;
	}	
}
