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

package alluxio.table.under.glue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.amazonaws.services.glue.model.Column;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class GlueUtilsTest {

  private static final String COLUMN_NAME = "testColumn";
  private static final String PARTITION_VALUE_1 = "value1";
  private static final String PARTITION_VALUE_2 = "value2";

  @Test
  public void toProtoSchema() {
    assertEquals(MockGlueDatabase.alluxioSchema(),
        GlueUtils.toProtoSchema(ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME))));
  }

  @Test
  public void toProto() {
    assertEquals(MockGlueDatabase.alluxioFieldSchema(),
        GlueUtils.toProto(ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME))));
  }

  @Test
  public void makePartitionName() throws IOException {
    List<Column> columns = ImmutableList.of(MockGlueDatabase.glueTestColumn(COLUMN_NAME));
    List<Column> emptyColumns = ImmutableList.of();

    List<String> partitionValueList1 = ImmutableList.of(PARTITION_VALUE_1);
    List<String> partitionValueList2 = ImmutableList.of(PARTITION_VALUE_1, PARTITION_VALUE_2);

    String expectedMessageEmpty =
        "Invalid partition key & values; key [], values [value1, ]";
    String expectedMessageUneven =
        "Invalid partition key & values; key [testColumn,], values [value1, value2, ]";

    assertEquals(
        GlueUtils.makePartitionName(columns, partitionValueList1),
        GlueUtils.makePartName(ImmutableList.of(COLUMN_NAME), partitionValueList1));
    assertMakePartName(emptyColumns, partitionValueList1, expectedMessageEmpty);
    assertMakePartName(columns, partitionValueList2, expectedMessageUneven);
  }

  private static void assertMakePartName(List<Column> columns,
      List<String> partitionValues, String expectedException) {
    IOException ioException = new IOException();
    try {
      GlueUtils.makePartitionName(columns, partitionValues);
    } catch (IOException e) {
      ioException = e;
    }
    assertExecption(ioException, expectedException);
  }

  private static void assertExecption(IOException ioExecption, String expectedException) {
    assertTrue(ioExecption.getMessage().contains(expectedException));
  }
}
