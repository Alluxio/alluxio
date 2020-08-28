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

import alluxio.table.common.udb.UdbConfiguration;
import alluxio.table.common.udb.UdbContext;
import alluxio.table.common.udb.UnderDatabase;
import alluxio.table.common.udb.UnderDatabaseFactory;

/**
 * Glue database factory to create database implementation.
 */
public class GlueDatabaseFactory implements UnderDatabaseFactory {
  public static final String TYPE = "glue";

  @Override
  public String getType() {
    return TYPE;
  }

  @Override
  public UnderDatabase create(UdbContext udbContext, UdbConfiguration configuration) {
    return GlueDatabase.create(udbContext, configuration);
  }
}
