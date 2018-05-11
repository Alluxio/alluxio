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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  /** Contain the checker results. */
  private final ConfigCheckReport mConfigCheckReport;
  /** Contain all the master configuration information. */
  private final ServerConfigurationRecord mMasterRecord;
  /** Contain all the worker configuration information. */
  private final ServerConfigurationRecord mWorkerRecord;

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
   */
  public static final class ConfigCheckReport {
    /** Record the configuration errors of last check conf. */
    private List<WrongProperty> mConfigErrors = new ArrayList<>();
    /** Record the configuration warnings of last check conf. */
    private List<WrongProperty> mConfigWarns = new ArrayList<>();
    /** Record the status of last check conf. */
    private Status mConfigStatus = Status.NOT_STARTED;

    /**
     * Creates a new instance of {@link ConfigCheckReport}.
     */
    private ConfigCheckReport() {}

    /**
     * @return a list of configuration errors
     */
    public List<WrongProperty> getConfigErrors() {
      return mConfigErrors;
    }

    /**
     * @return a list of configuration warnings
     */
    public List<WrongProperty> getConfigWarns() {
      return mConfigWarns;
    }

    /**
     * @return the status of the configuration checker results
     */
    public Status getConfigStatus() {
      return mConfigStatus;
    }

    /**
     * @param errors the errors to set
     * @return the configuration report
     */
    private ConfigCheckReport setConfigErrors(List<WrongProperty> errors) {
      mConfigErrors = Preconditions.checkNotNull(errors, "Cannot set null config errors");
      return this;
    }

    /**
     * @param warns the warnings to set
     * @return the configuration report
     */
    private ConfigCheckReport setConfigWarns(List<WrongProperty> warns) {
      mConfigWarns = Preconditions.checkNotNull(warns, "Cannot set null config warns");
      return this;
    }

    /**
     * @param status the status to set
     * @return the configuration report
     */
    private ConfigCheckReport setConfigStatus(Status status) {
      mConfigStatus = status;
      return this;
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
  public synchronized void checkConf() {
    // Generate the configuration map from master and worker configuration records
    Map<PropertyKey, Map<String, List<String>>> confMap = generateConfMap();

    // Update the errors and warnings configuration
    List<WrongProperty> configErrors = new ArrayList<>();
    List<WrongProperty> configWarns = new ArrayList<>();
    for (Map.Entry<PropertyKey, Map<String, List<String>>> entry : confMap.entrySet()) {
      WrongProperty wrongProperty = new WrongProperty()
          .setName(entry.getKey().getName()).setValues(entry.getValue());
      if (entry.getKey().getConsistencyLevel().equals(ConsistencyCheckLevel.ENFORCE)) {
        configErrors.add(wrongProperty);
      } else {
        configWarns.add(wrongProperty);
      }
    }

    mConfigCheckReport.setConfigErrors(configErrors).setConfigWarns(configWarns)
        .setConfigStatus(configErrors.size() > 0 ? Status.FAILED
        : configWarns.size() > 0 ? Status.WARN : Status.PASSED);
  }

  /**
   * @return the configuration checker report
   */
  public synchronized ConfigCheckReport getConfigCheckReport() {
    return mConfigCheckReport;
  }

  /**
   * Generates the configuration map to find wrong configuration.
   * The map is of format Map<PropertyKey, Map<Value, List<Hosts>>.
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
      Map<String, List<ConfigRecord>> recordMap) {
    for (Map.Entry<String, List<ConfigRecord>> record : recordMap.entrySet()) {
      String hostname = record.getKey();
      for (ConfigRecord conf : record.getValue()) {
        PropertyKey key = conf.getKey();
        if (key.getConsistencyLevel().equals(ConsistencyCheckLevel.IGNORE)) {
          continue;
        }
        String value = conf.getValue();
        targetMap.putIfAbsent(key, new HashMap<>());
        Map<String, List<String>> values = targetMap.get(key);
        values.putIfAbsent(value, new ArrayList<>());
        values.get(value).add(hostname);
      }
    }
  }
}
