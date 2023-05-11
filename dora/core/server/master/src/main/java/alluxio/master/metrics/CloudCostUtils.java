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

  /**
   * Cost map is a map between ufs operations and their associated cost in dollars per operation.
   * If it is a tiered storage solution, the top tier pricing is used.
   */
  // Aws pricing https://aws.amazon.com/s3/pricing/ last updated 02/19/2021
  public static final Map<UFSOps, Double> S3_COSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.005 / 1000,
          UFSOps.GET_FILE_INFO, 0.0004 / 1000,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.005 / 1000);
  // Azure data lake pricing https://azure.microsoft.com/en-us/pricing/details/storage/data-lake/ updated 02/19/2021
  public static final Map<UFSOps, Double> ABFS_COSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.0228 / 10000,
          UFSOps.GET_FILE_INFO, 0.00182 / 10000,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.0228 / 10000);
  // GCS pricing https://cloud.google.com/storage/pricing updated 02/19/2021
  public static final Map<UFSOps, Double> GCS_COSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.05 / 10000,
          UFSOps.GET_FILE_INFO, 0.004 / 10000,
          UFSOps.DELETE_FILE, 0.0,
          UFSOps.LIST_STATUS, 0.05 / 10000);
  // OSS pricing https://www.alibabacloud.com/product/oss/pricing updated 02/19/2021
  public static final Map<UFSOps, Double> OSS_COSTMAP =
      ImmutableMap.of(
          UFSOps.CREATE_FILE, 0.015629 / 10000,
          UFSOps.GET_FILE_INFO, 0.00100 / 10000,
          UFSOps.DELETE_FILE, 0.015629 / 10000,
          UFSOps.LIST_STATUS, 0.00100 / 1000);
  // A map mapping getUnderFSType to the cost map
  public static final Map<String, Map<UFSOps, Double>> COSTMAP =
      ImmutableMap.of(
          "abfs", ABFS_COSTMAP,
          "gcs", GCS_COSTMAP,
          "s3", S3_COSTMAP,
          "oss", OSS_COSTMAP
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
