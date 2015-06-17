package com.gome.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class ExplodeMap extends GenericUDTF {

	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub

	}

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		if (args.length != 1) {
			throw new UDFArgumentLengthException(
					"ExplodeMap takes only one argument");
		}
		if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
			throw new UDFArgumentException(
					"ExplodeMap takes string as a parameter");
		}

		ArrayList<String> fieldNames = new ArrayList<String>();
		ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		fieldNames.add("col1");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("col2");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		fieldNames.add("col3");
		fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

		return ObjectInspectorFactory.getStandardStructObjectInspector(
				fieldNames, fieldOIs);
	}

	public void process(Object[] args) throws HiveException {
		String input = args[0].toString();

		String[] tags;
		String result[] = new String[3];
		ArrayList<String> hastags = new ArrayList<String>();
		int count = 0;

		if (input.indexOf("order") == 0) {
			result[0] = "order";
			result[1] = "1";
			result[2] = "0";
			forward(result);
			return;
		} else if (input.indexOf("order") > 0) {
			input = input.substring(0, input.indexOf("order") - 1);
		}

		tags = input.split(",");

		if (tags.length == 1 && tags[0].equals("direct")) {
			result[0] = tags[0];
			result[1] = "1";
			result[2] = "0";
			forward(result);
			return;
		}

		int direct_flag = 0;
		for (int i = 0; i < tags.length; i++) {
			try {

				if (tags[i].equals("direct")) {
					direct_flag = 1;
					continue;
				}

				if (count == 4) {
					break;
				}

				if (!hastags.contains(tags[i])) {
					if (hastags.size() == 0) {
						result[0] = tags[i];
						result[1] = "1";
						result[2] = "0";
						hastags.add(tags[i]);
						count += 1;
						forward(result);
					} else {
						result[0] = tags[i];
						result[1] = "0";
						result[2] = "1";
						hastags.add(tags[i]);
						count += 1;
						forward(result);
					}
				}
			} catch (Exception ex) {
				continue;
			}
		}
		if (direct_flag > 0 && count < 4) {
			result[0] = "direct";
			result[1] = "0";
			result[2] = "1";
			forward(result);
		}

	}

}
