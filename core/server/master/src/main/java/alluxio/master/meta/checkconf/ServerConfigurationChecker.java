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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;

/**
 * This class is responsible for checking server-side configuration.
 */
public class ServerConfigurationChecker {
  /** Contain all the master configuration information. */
  private final ServerConfigurationRecord mMasterRecord;
  /** Contain all the worker configuration information. */
  private final ServerConfigurationRecord mWorkerRecord;

  /** Listeners to call when need to get master hostname. */
  private final BlockingQueue<Function<Long, String>> mGetMasterHostnameListeners
      = new LinkedBlockingQueue<>();
  /** Listeners to call when need to get worker hostname. */
  private final BlockingQueue<Function<Long, String>> mGetWorkerHostnameListeners
      = new LinkedBlockingQueue<>();

  /** Record the configuration errors of last check conf. */
  private List<WrongProperty> mConfErrors;
  /** Record the configuration warnings of last check conf. */
  private List<WrongProperty> mConfWarns;

  /**
   * Status of the check.
   */
  public enum Status {
    PASSED, // do not have configuration errors and warnings
    WARN, // do not have configuration errors but have warnings
    FAILED, // have configuration errors
    NOT_STARTED,
  }

  /** Record the status of last check conf. */
  private Status mStatus;

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
    mConfErrors = new ArrayList<>();
    mConfWarns = new ArrayList<>();
    mStatus = Status.NOT_STARTED;
  }

  /**
   * Checks the server-side configurations and records the check results.
   */
  public synchronized void checkConf() {
    // Generate the configuration map from master and worker configuration records
    Map<PropertyKey, Map<String, List<String>>> confMap = generateConfMap();

    // Update the errors and warnings configuration
    mConfErrors = new ArrayList<>();
    mConfWarns = new ArrayList<>();
    for (Map.Entry<PropertyKey, Map<String, List<String>>> entry : confMap.entrySet()) {
      WrongProperty wrongProperty = new WrongProperty()
          .setName(entry.getKey().getName()).setValues(entry.getValue());
      if (entry.getKey().getConsistencyLevel().equals(ConsistencyCheckLevel.ENFORCE)) {
        mConfErrors.add(wrongProperty);
      } else {
        mConfWarns.add(wrongProperty);
      }
    }

    // Update the status
    if (mConfErrors.size() > 0) {
      mStatus = Status.FAILED;
    } else if (mConfWarns.size() > 0) {
      mStatus = Status.WARN;
    } else {
      mStatus = Status.PASSED;
    }
  }

  /**
   * @return a list of configuration errors
   */
  public synchronized List<WrongProperty> getConfErrors() {
    return mConfErrors;
  }

  /**
   * @return a list of configuration warnings
   */
  public synchronized List<WrongProperty> getConfWarns() {
    return mConfWarns;
  }

  /**
   * @return the server-side configuration check status
   */
  public synchronized Status getStatus() {
    return mStatus;
  }

  /**
   * Generates the configuration map to find wrong configuration.
   * The map is of format Map<PropertyKey, Map<Value, List<Hosts>>.
   *
   * @return the generated configuration map
   */
  private Map<PropertyKey, Map<String, List<String>>> generateConfMap() {
    Map<PropertyKey, Map<String, List<String>>> confMap = new HashMap<>();
    for (Map.Entry<Long, List<ConfigRecord>> record : mMasterRecord.getConfMap().entrySet()) {
      String hostname = mGetMasterHostnameListeners.iterator().next().apply(record.getKey());
      fillConfMap(confMap, record.getValue(), hostname);
    }

    for (Map.Entry<Long, List<ConfigRecord>> record : mWorkerRecord.getConfMap().entrySet()) {
      String hostname = mGetWorkerHostnameListeners.iterator().next().apply(record.getKey());
      fillConfMap(confMap, record.getValue(), hostname);
    }
    return confMap;
  }

  /**
   * Fills the configuration map.
   *
   * @param map the map to put data to
   * @param recordList the records to get information from
   * @param hostname the hostname of the configuration to put
   */
  private void fillConfMap(Map<PropertyKey, Map<String, List<String>>> map,
      List<ConfigRecord> recordList, String hostname) {
    for (ConfigRecord record : recordList) {
      PropertyKey key = record.getKey();
      if (key.getConsistencyLevel().equals(ConsistencyCheckLevel.IGNORE)) {
        continue;
      }
      String value = record.getValue();
      map.putIfAbsent(key, new HashMap<>());
      Map<String, List<String>> values = map.get(key);
      values.putIfAbsent(value, new ArrayList<>());
      values.get(value).add(hostname);
    }
  }

  /**
   * Registers callback functions to use when need to get master hostname.
   *
   * @param function the function to register
   */
  public void registerGetMasterHostnameListener(Function<Long, String> function) {
    mGetMasterHostnameListeners.add(function);
  }

  /**
   * Registers callback functions to use when need to get worker hostname.
   *
   * @param function the function to register
   */
  public void registerGetWorkerHostnameListener(Function<Long, String> function) {
    mGetWorkerHostnameListeners.add(function);
  }
}
