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

package alluxio.table.common.layout;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import alluxio.grpc.table.ColumnStatisticsInfo;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.grpc.table.layout.hive.Storage;
import alluxio.table.common.Layout;
import alluxio.table.common.LayoutRegistry;
import alluxio.util.CommonUtils;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class HiveLayoutTest {

  @Test
  public void toProto() throws Exception {
    HiveLayout layout = createRandom();
    assertNotNull(layout.toProto());
  }

  @Test
  public void factoryCreate() throws Exception {
    HiveLayout layout = createRandom();
    assertNotNull(new HiveLayout.HiveLayoutFactory().create(layout.toProto()));
  }

  @Test
  public void registryCreate() throws Exception {
    HiveLayout layout = createRandom();
    assertNotNull(new HiveLayout.HiveLayoutFactory().create(layout.toProto()));

    LayoutRegistry registry = new LayoutRegistry();
    registry.refresh();
    Layout instance = registry.create(layout.toProto());
    assertNotNull(instance);
    assertEquals(layout.toProto(), instance.toProto());
  }

  @Test
  public void factoryConversions() throws Exception {
    HiveLayout layout = createRandom();
    alluxio.grpc.table.Layout layoutProto = layout.toProto();
    Layout layout2 = new HiveLayout.HiveLayoutFactory().create(layoutProto);
    alluxio.grpc.table.Layout layout2Proto = layout2.toProto();
    assertEquals(layoutProto, layout2Proto);
  }

  private HiveLayout createRandom() {
    PartitionInfo.Builder pib = PartitionInfo.newBuilder();

    pib.setDbName(CommonUtils.randomAlphaNumString(10));
    pib.setTableName(CommonUtils.randomAlphaNumString(10));
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(1, 5); i++) {
      pib.addDataCols(
          FieldSchema.newBuilder().setName(CommonUtils.randomAlphaNumString(10)).build());
    }
    pib.setPartitionName(CommonUtils.randomAlphaNumString(10));
    pib.setStorage(Storage.newBuilder().setLocation(CommonUtils.randomAlphaNumString(10)).build());
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(0, 5); i++) {
      pib.addValues(CommonUtils.randomAlphaNumString(10));
    }

    List<ColumnStatisticsInfo> stats = new ArrayList<>();
    for (int i = 0; i < ThreadLocalRandom.current().nextInt(0, 5); i++) {
      stats.add(ColumnStatisticsInfo.newBuilder().setColName(CommonUtils.randomAlphaNumString(10))
          .setColType(CommonUtils.randomAlphaNumString(10)).build());
    }

    return new HiveLayout(pib.build(), stats);
  }
}
