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

package alluxio.master.catalog;

import alluxio.grpc.FileStatistics;
import alluxio.grpc.Schema;
import alluxio.table.common.TableView;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A version of a table.
 */
public class TableVersion {
  public static final String DEFAULT_VIEW_NAME = "default";
  private final Schema mSchema;
  /** this maps names to views of the table. Views are read-only per version. */
  private final Map<String, TableView> mViews;

  /**
   * Creates an instance.
   *
   * @param schema the table schema
   */
  public TableVersion(Schema schema) {
    mSchema = schema;
    mViews = new ConcurrentHashMap<>();
    mViews.size(); // TODO(gpang): read the field
  }

  /**
   * @param name the name of the view to add
   * @param view the view to add
   */
  public void addView(String name, TableView view) {
    mViews.putIfAbsent(name, view);
  }

  /**
   * @return the table schema
   */
  public Schema getSchema() {
    return mSchema;
  }

  /**
   * @return the base location
   */
  public String getBaseLocation() {
    // TODO(gpang): remove api, make part of views
    TableView view = mViews.get(DEFAULT_VIEW_NAME);
    if (view != null) {
      return view.getBaseLocation();
    }
    return "n/a";
  }

  /**
   * @return the statistics
   */
  public Map<String, FileStatistics> getStatistics() {
    // TODO(gpang): remove api, make part of views
    TableView view = mViews.get(DEFAULT_VIEW_NAME);
    if (view != null) {
      return view.getStatistics();
    }
    return Collections.emptyMap();
  }
}
