/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.job.plan.transform.format.orc;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.HiveConstants;
import alluxio.job.plan.transform.format.TableRow;
import alluxio.job.plan.transform.format.csv.Decimal;
import alluxio.job.plan.transform.format.parquet.ParquetRow;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VoidColumnVector;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A row in a Orc table.
 */
public class OrcRow implements TableRow {
  private final VectorizedRowBatch mBatch;
  private final int mPosition;
  private final Map<String, Integer> mColumnNamePosition;

  private final OrcSchema mSchema;

  /**
   * Constructor for OrcRow.
   * @param schema the schema
   * @param batch the vectorized row batch
   * @param position the row position inside the vectorized row batch
   * @param fieldNames ordered list of field names
   */
  public OrcRow(OrcSchema schema, VectorizedRowBatch batch, int position,
                List<String> fieldNames) {
    mSchema = schema;
    mBatch = batch;
    mPosition = position;
    mColumnNamePosition = new HashMap<>();

    for (int i = 0; i < fieldNames.size(); i++) {
      final String fieldName = fieldNames.get(i);

      mColumnNamePosition.put(fieldName, i);
    }
  }

  @Override
  public ParquetRow toParquet() throws IOException {
    Schema writeSchema = mSchema.getWriteSchema();
    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(writeSchema);
    for (FieldSchema field : mSchema.getAlluxioSchema()) {
      String name = field.getName();
      String type = field.getType();
      Object value = getColumn(name);
      recordBuilder.set(writeSchema.getField(name), convert(value, name, type));
    }
    return new ParquetRow(recordBuilder.build());
  }

  @Override
  public Object getColumn(String column) {
    final Integer columnPosition = mColumnNamePosition.get(column);

    if (columnPosition == null) {
      throw new IllegalArgumentException("Invalid column name: " + column);
    }

    final ColumnVector col = mBatch.cols[columnPosition];

    if (col.isNull[mPosition]) {
      return null;
    }

    if (col instanceof TimestampColumnVector) {
      return ((TimestampColumnVector) col).asScratchTimestamp(mPosition).getTime();
    } else if (col instanceof VoidColumnVector) {
      return null;
    } else if (col instanceof DecimalColumnVector) {
      final HiveDecimal hiveDecimal = ((DecimalColumnVector) col).vector[mPosition]
          .getHiveDecimal();
      return hiveDecimal;
    } else if (col instanceof LongColumnVector) {
      return ((LongColumnVector) col).vector[mPosition];
    } else if (col instanceof BytesColumnVector) {
      BytesColumnVector bcv = (BytesColumnVector) col;
      return Arrays.copyOfRange(bcv.vector[mPosition], bcv.start[mPosition],
          bcv.start[mPosition] + bcv.length[mPosition]);
    } else if (col instanceof DoubleColumnVector) {
      return ((DoubleColumnVector) col).vector[mPosition];
    }

    throw new UnsupportedOperationException("Unsupported column vector: "
        + col.getClass().getName());
  }

  private Object convert(Object value, String name, String type) throws IOException {
    if (value == null) {
      return null;
    }

    switch (HiveConstants.Types.getHiveConstantType(type)) {
      case HiveConstants.Types.DECIMAL:
        final Decimal decimal = new Decimal(type);
        return ((HiveDecimal) value).bigIntegerBytesScaled(decimal.getScale());
      case HiveConstants.Types.VARCHAR:
      case HiveConstants.Types.CHAR:
      case HiveConstants.Types.STRING:
        return new String((byte[]) value);
      default:
        return value;
    }
  }
}
