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

package alluxio.master.meta.checkconf;

import alluxio.PropertyKey;
import alluxio.PropertyKey.ConsistencyCheckLevel;
import alluxio.wire.Address;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigCheckReport.ConfigStatus;
import alluxio.wire.InconsistentProperty;
import alluxio.wire.Scope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  /** Contain all the master configuration information. */
  private final ServerConfigurationStore mMasterStore;
  /** Contain all the worker configuration information. */
  private final ServerConfigurationStore mWorkerStore;
  /** Contain the checker results. */
  private ConfigCheckReport mConfigCheckReport;

  /**
   * Constructs a new {@link ServerConfigurationChecker}.
   *
   * @param masterStore master configuration store
   * @param workerStore worker configuration store
   */
  public ServerConfigurationChecker(ServerConfigurationStore masterStore,
      ServerConfigurationStore workerStore) {
    mMasterStore = masterStore;
    mWorkerStore = workerStore;
    mConfigCheckReport = new ConfigCheckReport();
    mMasterStore.registerChangeListener(this::regenerateReport);
  }

  /**
   * Checks the server-side configurations and records the check results.
   */
  public synchronized void regenerateReport() {
    // Generate the configuration map from master and worker configuration records
    Map<PropertyKey, Map<String, List<String>>> confMap = generateConfMap();

    // Update the errors and warnings configuration
    Map<Scope, List<InconsistentProperty>> confErrors = new HashMap<>();
    Map<Scope, List<InconsistentProperty>> confWarns = new HashMap<>();

    for (Map.Entry<PropertyKey, Map<String, List<String>>> entry : confMap.entrySet()) {
      if (entry.getValue().size() >= 2) {
        PropertyKey key = entry.getKey();
        InconsistentProperty inconsistentProperty = new InconsistentProperty()
            .setName(key.getName()).setValues(entry.getValue());
        Scope scope = key.getScope().equals(Scope.ALL) ? Scope.SERVER : key.getScope();
        if (entry.getKey().getConsistencyLevel().equals(ConsistencyCheckLevel.ENFORCE)) {
          confErrors.putIfAbsent(scope, new ArrayList<>());
          confErrors.get(scope).add(inconsistentProperty);
        } else {
          confWarns.putIfAbsent(scope, new ArrayList<>());
          confWarns.get(scope).add(inconsistentProperty);
        }
      }
    }

    // Update configuration status
    ConfigStatus status = confErrors.values().stream().anyMatch(a -> a.size() > 0)
        ? ConfigStatus.FAILED : confWarns.values().stream().anyMatch(a -> a.size() > 0)
        ? ConfigStatus.WARN : ConfigStatus.PASSED;

    mConfigCheckReport = new ConfigCheckReport(confErrors, confWarns, status);
  }

  /**
   * @return the configuration checker report
   */
  public synchronized ConfigCheckReport getConfigCheckReport() {
    return mConfigCheckReport;
  }

  /**
   * Generates the configuration map to find wrong configuration.
   * The map is of format Map<PropertyKey, Map<Value, List<Host:Port>>.
   *
   * @return the generated configuration map
   */
  private Map<PropertyKey, Map<String, List<String>>> generateConfMap() {
    Map<PropertyKey, Map<String, List<String>>> confMap = new HashMap<>();
    fillConfMap(confMap, mMasterStore.getConfMap());
    fillConfMap(confMap, mWorkerStore.getConfMap());
    return confMap;
  }

  /**
   * Fills the configuration map.
   *
   * @param targetMap the map to fill
   * @param recordMap the map to get data from
   */
  private void fillConfMap(Map<PropertyKey, Map<String, List<String>>> targetMap,
      Map<Address, List<ConfigRecord>> recordMap) {
    for (Map.Entry<Address, List<ConfigRecord>> record : recordMap.entrySet()) {
      Address address = record.getKey();
      String addressStr = String.format("%s:%s", address.getHost(), address.getRpcPort());
      for (ConfigRecord conf : record.getValue()) {
        PropertyKey key = conf.getKey();
        if (key.getConsistencyLevel() == ConsistencyCheckLevel.IGNORE) {
          continue;
        }
        String value = conf.getValue();
        targetMap.putIfAbsent(key, new HashMap<>());
        Map<String, List<String>> values = targetMap.get(key);
        values.putIfAbsent(value, new ArrayList<>());
        values.get(value).add(addressStr);
      }
    }
  }
}
