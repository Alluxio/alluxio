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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.PropertyKey.Scope;
import alluxio.PropertyKey.ConsistencyCheckLevel;
import alluxio.wire.Address;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  private static final Logger LOG = LoggerFactory.getLogger(ServerConfigurationChecker.class);
  private static final String HELP_INFO = "For details, please visit Alluxio web UI or"
      + "run fsadmin doctor CLI.";
  private static final long MAX_LOG_INTERVAL
      = Configuration.getMs(PropertyKey.MASTER_CONFIG_REPORT_MAX_LOG_INTERVAL_MS);
  /** Contain all the master configuration information. */
  private final ServerConfigurationStore mMasterStore;
  /** Contain all the worker configuration information. */
  private final ServerConfigurationStore mWorkerStore;
  /** Contain the checker results. */
  private ConfigCheckReport mConfigCheckReport;
  /** The last time we logged the config checker report. */
  private long mLastLogConfigReportTimeMs;

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
   * SERVER, MASTER and WORKER. Scope.ALL will be considered as Scope.SERVER.
   */
  public static final class ConfigCheckReport {
    /** Record the configuration errors. */
    private final Map<Scope, List<InconsistentProperty>> mConfigErrors;
    /** Record the configuration warnings. */
    private final Map<Scope, List<InconsistentProperty>> mConfigWarns;
    /** Record the overall status of config check report. */
    private final Status mStatus;

    /**
     * Creates a new instance of {@link ConfigCheckReport}.
     */
    private ConfigCheckReport() {
      mConfigErrors = new HashMap<>();
      mConfigWarns = new HashMap<>();
      mStatus = Status.NOT_STARTED;
    }

    /**
     * Creates a new instance of {@link ConfigCheckReport}.
     *
     * @param configErrors the configuration errors
     * @param configWarns the configuration warnings
     * @param status the overall report status
     */
    private ConfigCheckReport(Map<Scope, List<InconsistentProperty>> configErrors,
        Map<Scope, List<InconsistentProperty>> configWarns, Status status) {
      mConfigErrors = configErrors;
      mConfigWarns = configWarns;
      mStatus = status;
    }

    /**
     * @return a map of configuration errors
     */
    public Map<Scope, List<InconsistentProperty>> getConfigErrors() {
      return mConfigErrors;
    }

    /**
     * @return a map of configuration warnings
     */
    public Map<Scope, List<InconsistentProperty>> getConfigWarns() {
      return mConfigWarns;
    }

    /**
     * @return the overall report status
     */
    public Status getStatus() {
      return mStatus;
    }
  }

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
    mLastLogConfigReportTimeMs = System.currentTimeMillis();
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

    Status status = confErrors.values().stream().anyMatch(a -> a.size() > 0) ? Status.FAILED
        : confWarns.values().stream().anyMatch(a -> a.size() > 0) ? Status.WARN : Status.PASSED;

    if (!status.equals(mConfigCheckReport.getStatus())) {
      logConfigReport();
    }

    mConfigCheckReport = new ConfigCheckReport(confErrors, confWarns, status);
  }

  /**
   * @return the configuration checker report
   */
  public synchronized ConfigCheckReport getConfigCheckReport() {
    return mConfigCheckReport;
  }

  /**
   * Logs the configuration check report information.
   */
  public synchronized void logConfigReport() {
    Status reportStatus = mConfigCheckReport.getStatus();
    if (reportStatus.equals(Status.PASSED)) {
      LOG.info("Stauts: {}", reportStatus);
    } else if (reportStatus.equals(Status.WARN)) {
      LOG.warn("Status: {}. {}", reportStatus, HELP_INFO);
      LOG.warn("Warnings: {}", mConfigCheckReport.getConfigWarns().values().stream()
          .map(Object::toString).collect(Collectors.joining(", ")));
    } else {
      LOG.error("Status: {}. {}", reportStatus, HELP_INFO);
      LOG.error("Errors: {}", mConfigCheckReport.getConfigErrors().values().stream()
          .map(Object::toString).collect(Collectors.joining(", ")));
      LOG.warn("Warnings: {}", mConfigCheckReport.getConfigWarns().values().stream()
          .map(Object::toString).collect(Collectors.joining(", ")));
    }
    mLastLogConfigReportTimeMs = System.currentTimeMillis();
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

  /**
   * Checks if the time gap between current time and the last time that
   * we logged the configuration report is bigger than the maximum log interval.
   * If it is bigger, we log the configuration report.
   */
  public synchronized void checkAndLog() {
    long logInterval = System.currentTimeMillis() - mLastLogConfigReportTimeMs;
    if (logInterval > MAX_LOG_INTERVAL) {
      logConfigReport();
    }
  }
}
