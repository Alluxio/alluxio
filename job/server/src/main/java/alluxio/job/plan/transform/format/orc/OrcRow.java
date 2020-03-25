/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package alluxio.job.plan.transform.format.orc;

import alluxio.job.plan.transform.FieldSchema;
import alluxio.job.plan.transform.format.TableRow;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OrcRow implements TableRow {
  private final VectorizedRowBatch mBatch;
  private final int mPosition;
  private final Map<String, Integer> mColumnNamePosition;

  private final OrcSchema mSchema;

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
      //String type = field.getType();
      Object value = getColumn(name);
      recordBuilder.set(writeSchema.getField(name), value);
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
      return ((TimestampColumnVector) col).getTimestampAsLong(mPosition);
    } else if (col instanceof VoidColumnVector) {
      return null;
    } else if (col instanceof DecimalColumnVector) {
      final HiveDecimal hiveDecimal = ((DecimalColumnVector) col).vector[mPosition].getHiveDecimal();
      return hiveDecimal;
    } else if (col instanceof LongColumnVector) {
      return ((LongColumnVector) col).vector[mPosition];
    } else if (col instanceof BytesColumnVector) {
      return ((BytesColumnVector) col).vector[mPosition];
    } else if (col instanceof DoubleColumnVector) {
      return ((DoubleColumnVector) col).vector[mPosition];
    }

    throw new UnsupportedOperationException("Unsupported column vector: " + col.getClass().getName());
  }
}
