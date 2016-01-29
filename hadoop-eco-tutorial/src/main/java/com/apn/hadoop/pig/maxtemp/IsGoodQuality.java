package com.apn.hadoop.pig.maxtemp;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.pig.FilterFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;

/**
 * The Class IsGoodQuality.
 *
 * @author amit.nema
 */
public class IsGoodQuality extends FilterFunc {

	private static final Object[] VALUES_TO_FILTER = { 0, 1, 4, 5, 9 };

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.apache.pig.EvalFunc#exec(org.apache.pig.data.Tuple)
	 */
	@Override
	public Boolean exec(Tuple tuple) throws IOException {
		Boolean exec = false;
		if (tuple != null && tuple.size() > 0 && !tuple.isNull(0)) {
			Object object = tuple.get(0);
			if (object instanceof Integer) {
				Integer integer = (Integer) object;
				exec = ArrayUtils.contains(VALUES_TO_FILTER, integer);
			}
		}
		return exec;
	}

	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcSpecs = new ArrayList<FuncSpec>();
		FuncSpec funcSpec = new FuncSpec(this.getClass().getName(),
				new Schema(new Schema.FieldSchema(null, DataType.INTEGER)));
		funcSpecs.add(funcSpec);
		return funcSpecs;
	}
}
