import org.apache.hadoop.hdfs.util.Diff.ListType
import org.apache.spark.ml.linalg.{DenseVector, SQLDataTypes, Vectors}
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/**
  * Created by pi on 17-9-27.
  */
object FeatureAgg extends UserDefinedAggregateFunction {
  // Data types of input arguments of this aggregate function
  def inputSchema: StructType = StructType(StructField("inputColumn", StringType) :: Nil)
  // Data types of values in the aggregation buffer
  def bufferSchema: StructType = {
    StructType(StructField("sum", StringType) :: Nil)
  }
  // The data type of the returned value
  def dataType: DataType = StringType
  // Whether this function always returns the same output on the identical input
  def deterministic: Boolean = true
  // Initializes the given aggregation buffer. The buffer itself is a `Row` that in addition to
  // standard methods like retrieving a value at an index (e.g., get(), getBoolean()), provides
  // the opportunity to update its values. Note that arrays and maps inside the buffer are still
  // immutable.
  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }
  // Updates the given aggregation buffer `buffer` with new input data from `input`
  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
//      buffer(0) = buffer.getAs[DenseVector](0) + input.getAs[DenseVector](0)
      val v1 = buffer.getString(0)
      val v2 = input.getString(0)
      if(!v1.equals("") && !v2.equals("")) buffer(0) = v1+","+v2
      else if(!v1.equals("")) buffer(0) = v1
      else if(!v2.equals("")) buffer(0) = v2
      else buffer(0) = ""

    }
  }
  // Merges two aggregation buffers and stores the updated buffer values back to `buffer1`
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    val v1 = buffer1.getString(0)
    val v2 = buffer2.getString(0)
    if(!v1.equals("") && !v2.equals("")) buffer1(0) = v1+","+v2
    else if(!v1.equals("")) buffer1(0) = v1
    else if(!v2.equals("")) buffer1(0) = v2
    else buffer1(0) = ""
  }
  // Calculates the final result
  def evaluate(buffer: Row): String = {
    //buffer.getLong(0).toDouble / buffer.getLong(1)
    buffer.getString(0)
  }
}
