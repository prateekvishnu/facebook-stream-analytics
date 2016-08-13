package spark.simple.transformers;

import org.apache.pig.data.DataType;
import org.apache.spark.ml.Transformer;
import org.apache.spark.ml.param.ParamMap;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class LabelTransformer extends Transformer {

	public String uid() {
		// Not needed now
		return null;
	}

	@Override
	public Transformer copy(ParamMap arg0) {
		// Not needed now
		return null;
	}

	@Override
	public DataFrame transform(DataFrame df) {
		//if()
		return df.withColumn("label", df.col("labelId")
				 .cast(DataTypes.DoubleType))
				 .drop("labelId");
	}

	@Override
	public StructType transformSchema(StructType arg0) {
		// Not needed now
		return null;
	}

}
