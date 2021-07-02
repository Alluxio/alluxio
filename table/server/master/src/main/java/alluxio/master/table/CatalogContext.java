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

import alluxio.table.common.LayoutRegistry;
import alluxio.table.common.udb.UnderDatabaseRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The context for the catalog.
 */
public class CatalogContext {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogContext.class);

  private final UnderDatabaseRegistry mUdbRegistry;
  private final LayoutRegistry mLayoutRegistry;

  /**
   * Creates an instance.
   *
   * @param udbRegistry the udb registry
   * @param layoutRegistry the layout registry
   */
  public CatalogContext(UnderDatabaseRegistry udbRegistry, LayoutRegistry layoutRegistry) {
    mUdbRegistry = udbRegistry;
    mLayoutRegistry = layoutRegistry;
  }

  /**
   * @return the layout registry
   */
  public LayoutRegistry getLayoutRegistry() {
    return mLayoutRegistry;
  }

  /**
   * @return the udb registry
   */
  public UnderDatabaseRegistry getUdbRegistry() {
    return mUdbRegistry;
  }
}
