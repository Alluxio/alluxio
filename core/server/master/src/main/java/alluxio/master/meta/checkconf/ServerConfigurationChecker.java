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
import alluxio.PropertyKey.Scope;
import alluxio.PropertyKey.ConsistencyCheckLevel;
import alluxio.wire.Address;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  /** Contain all the master configuration information. */
  private final ServerConfigurationRecord mMasterRecord;
  /** Contain all the worker configuration information. */
  private final ServerConfigurationRecord mWorkerRecord;
  /** Contain the checker results. */
  private ConfigCheckReport mConfigCheckReport;

  /**
   * Status of the check.
   */
  public enum Status {
    PASSED, // do not have configuration errors and warnings
    WARN, // do not have configuration errors but have warnings
    FAILED, // have configuration errors
    NOT_STARTED,
  }

  /**
   * Represents a configuration report which records the configuration checker results.
   * Since we check server-side configuration, Scope here only includes
   * SERVER, MASTER and WORKER.
   */
  public static final class ConfigCheckReport {
    /** Record the configuration errors of last check conf. */
    private Map<Scope, List<WrongProperty>> mConfigErrors;
    /** Record the configuration warnings of last check conf. */
    private Map<Scope, List<WrongProperty>> mConfigWarns;
    /** Record the status of last check conf. */
    private Map<Scope, Status> mConfigStatus;

    /**
     * Creates a new instance of {@link ConfigCheckReport}.
     */
    private ConfigCheckReport() {
      mConfigErrors = new HashMap<>();
      mConfigWarns = new HashMap<>();
      mConfigStatus = new HashMap<>();
      mConfigStatus.put(Scope.SERVER, Status.NOT_STARTED);
      mConfigStatus.put(Scope.MASTER, Status.NOT_STARTED);
      mConfigStatus.put(Scope.WORKER, Status.NOT_STARTED);
    }

    /**
     * Creates a new instance of {@link ConfigCheckReport}.
     *
     * @param configErrors the configuration errors
     * @param configWarns the configuration warnings
     * @param configStatus the configuration status
     */
    private ConfigCheckReport(Map<Scope, List<WrongProperty>> configErrors,
        Map<Scope, List<WrongProperty>> configWarns, Map<Scope, Status> configStatus) {
      mConfigErrors = configErrors;
      mConfigWarns = configWarns;
      mConfigStatus = configStatus;
    }

    /**
     * @return a map of configuration errors
     */
    public Map<Scope, List<WrongProperty>> getConfigErrors() {
      return mConfigErrors;
    }

    /**
     * @return a map of configuration warnings
     */
    public Map<Scope, List<WrongProperty>> getConfigWarns() {
      return mConfigWarns;
    }

    /**
     * @return the status of the configuration checker results
     */
    public Map<Scope, Status> getConfigStatus() {
      return mConfigStatus;
    }
  }

  /**
   * Constructs a new {@link ServerConfigurationChecker}.
   *
   * @param masterRecord master configuration record
   * @param workerRecord worker configuration record
   */
  public ServerConfigurationChecker(ServerConfigurationRecord masterRecord,
      ServerConfigurationRecord workerRecord) {
    mMasterRecord = masterRecord;
    mWorkerRecord = workerRecord;
    mConfigCheckReport = new ConfigCheckReport();
  }

  /**
   * Checks the server-side configurations and records the check results.
   */
  public synchronized void regenerateReport() {
    // Generate the configuration map from master and worker configuration records
    Map<PropertyKey, Map<String, List<String>>> confMap = generateConfMap();

    // Update the errors and warnings configuration
    Map<Scope, List<WrongProperty>> confErrors = new HashMap<>();
    Map<Scope, List<WrongProperty>> confWarns = new HashMap<>();

    for (Map.Entry<PropertyKey, Map<String, List<String>>> entry : confMap.entrySet()) {
      if (entry.getValue().size() >= 2) {
        PropertyKey key = entry.getKey();
        WrongProperty wrongProperty = new WrongProperty()
            .setName(key.getName()).setValues(entry.getValue());
        Scope scope = key.getScope().equals(Scope.ALL) ? Scope.SERVER : key.getScope();
        if (entry.getKey().getConsistencyLevel().equals(ConsistencyCheckLevel.ENFORCE)) {
          confErrors.putIfAbsent(scope, new ArrayList<>());
          confErrors.get(scope).add(wrongProperty);
        } else {
          confWarns.putIfAbsent(scope, new ArrayList<>());
          confWarns.get(scope).add(wrongProperty);
        }
      }
    }

    // Update configuration status
    Map<Scope, Status> configStatus = new HashMap<>();
    Status masterStatus = Status.PASSED;
    Status workerStatus = Status.PASSED;

    for (Map.Entry<Scope, List<WrongProperty>> entry : confWarns.entrySet()) {
      if (entry.getValue().size() > 0) {
        Scope scope = entry.getKey();
        if (scope.contains(Scope.MASTER)) {
          masterStatus = Status.WARN;
        }
        if (scope.contains(Scope.WORKER)) {
          workerStatus = Status.WARN;
        }
      }
    }
    for (Map.Entry<Scope, List<WrongProperty>> entry : confErrors.entrySet()) {
      if (entry.getValue().size() > 0) {
        Scope scope = entry.getKey();
        if (scope.contains(Scope.MASTER)) {
          masterStatus = Status.FAILED;
        }
        if (scope.contains(Scope.WORKER)) {
          workerStatus = Status.FAILED;
        }
      }
    }

    configStatus.put(Scope.MASTER, masterStatus);
    configStatus.put(Scope.WORKER, workerStatus);
    configStatus.put(Scope.SERVER,
        (masterStatus.equals(Status.FAILED) || workerStatus.equals(Status.FAILED)) ? Status.FAILED
        : (masterStatus.equals(Status.WARN) || workerStatus.equals(Status.WARN)) ? Status.WARN
        : Status.PASSED);

    mConfigCheckReport = new ConfigCheckReport(confErrors, confWarns, configStatus);
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
    fillConfMap(confMap, mMasterRecord.getConfMap());
    fillConfMap(confMap, mWorkerRecord.getConfMap());
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
        if (key.getConsistencyLevel().equals(ConsistencyCheckLevel.IGNORE)) {
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
