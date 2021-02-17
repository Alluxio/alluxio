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

package alluxio.master.metrics;

import alluxio.master.file.DefaultFileSystemMaster.Metrics.UFSOps;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * This class contains utilities to compute cloud cost savings.
 */
public final class CloudCostUtils {
  private CloudCostUtils() {}  // prevent instantiation

  public static final Map<UFSOps, Double> S3COSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.000005,
          UFSOps.GET_FILE_INFO, 0.000005,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.000005);
  public static final Map<UFSOps, Double> ABFSCOSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.0000065,
          UFSOps.GET_FILE_INFO, 0.0000065,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.0000065);
  public static final Map<UFSOps, Double> GCSCOSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.000005,
          UFSOps.GET_FILE_INFO, 0.0000004,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.000005);
  public static final Map<UFSOps, Double> OSSCOSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.000005,
          UFSOps.GET_FILE_INFO, 0.0000004,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.000005);
  public static final Map<String, Map<UFSOps, Double>> COSTMAP =
      ImmutableMap.of(
       "abfs", ABFSCOSTMAP,
       "gcs", GCSCOSTMAP,
       "s3", S3COSTMAP
      );

  /**
   * Calculate the saved cost from the perUfs Operations saved map.
   *
   * @param ufsType ufs type could be s3, gcs, abfs etc
   * @param perUfsMap a map mapping operations to the number of ops saved
   * @return total cost saved from the saved metadata operations
   */
  public static double calculateCost(String ufsType, Map<String, Long> perUfsMap) {
    double sum = 0;
    if (!COSTMAP.containsKey(ufsType)) {
      return 0;
    }
    Map<UFSOps, Double> costMap = COSTMAP.get(ufsType);
    for (Map.Entry<String, Long> entry : perUfsMap.entrySet()) {
      try {
        sum += costMap.getOrDefault(UFSOps.valueOf(entry.getKey()), 0.0) * entry.getValue();
      } catch (IllegalArgumentException ignored) {
        // intentionally left blank
      }
    }
    return sum;
  }
}
