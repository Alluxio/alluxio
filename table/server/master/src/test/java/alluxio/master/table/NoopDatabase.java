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

package alluxio.master.table;

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UdbTable;
import alluxio.table.common.udb.UnderDatabase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * A udb implementation which does nothing, used for testing.
 */
public class NoopDatabase implements UnderDatabase {
  private static final String DB_NAME = "noop_udb_name";

  private final UdbContext mUdbContext;
  private final UdbConfiguration mConfiguration;

  private NoopDatabase(UdbContext udbContext, UdbConfiguration configuration) {
    mUdbContext = udbContext;
    mConfiguration = configuration;
  }

  /**
   * Creates an instance.
   *
   * @param udbContext the db context
   * @param configuration the configuration
   * @return the new instance
   */
  public static NoopDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    return new NoopDatabase(udbContext, configuration);
  }

  @Override
  public String getType() {
    return NoopUdbFactory.TYPE;
  }

  @Override
  public String getName() {
    return DB_NAME;
  }

  @Override
  public List<String> getTableNames() throws IOException {
    return Collections.emptyList();
  }

  @Override
  public UdbTable getTable(String tableName) throws IOException {
    throw new IOException(String.format("NoopDb Table %s does not exist.", tableName));
  }
}
