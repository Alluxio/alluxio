/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.perf.basic;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import alluxio.perf.conf.PerfConf;
import alluxio.perf.util.SAXConfiguration;

/**
 * Manage the configurations for each task.
 */
public class TaskConfiguration {
  public static final boolean DEFAULT_BOOLEAN = false;
  public static final int DEFAULT_INTEGER = 0;
  public static final long DEFAULT_LONG = 0;
  public static final String DEFAULT_STRING = "";
  private static final Logger LOG = Logger.getLogger("");

  private static TaskConfiguration sTaskConf = null;

  /**
   * Get the configuration.
   * 
   * @param type the type of the benchmark task
   * @param fromFile if true, it will load the configuration file, otherwise it return an empty
   *        configuration.
   * @return the task configuration
   */
  public static synchronized TaskConfiguration get(String type, boolean fromFile) {
    if (sTaskConf == null) {
      if (fromFile) {
        try {
          sTaskConf =
              new TaskConfiguration(PerfConf.get().ALLUXIO_PERF_HOME + "/conf/testsuite/" + type
                  + ".xml");
        } catch (Exception e) {
          LOG.error("Error when parse conf/testsuite/" + type + ".xml", e);
          throw new RuntimeException("Failed to parse conf/testsuite/" + type + ".xml");
        }
      } else {
        sTaskConf = new TaskConfiguration();
      }
    }
    return sTaskConf;
  }

  private Map<String, String> mProperties;

  protected TaskConfiguration() {
    mProperties = new HashMap<String, String>();
  }

  protected TaskConfiguration(String xmlFileName) throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser saxParser = spf.newSAXParser();
    File xmlFile = new File(xmlFileName);
    SAXConfiguration saxConfiguration = new SAXConfiguration();
    saxParser.parse(xmlFile, saxConfiguration);
    mProperties = saxConfiguration.getProperties();
  }

  public synchronized void addProperty(String name, String value) {
    mProperties.put(name, value);
  }

  public synchronized Map<String, String> getAllProperties() {
    Map<String, String> ret = new HashMap<String, String>(mProperties.size());
    ret.putAll(mProperties);
    return ret;
  }

  public synchronized boolean getBooleanProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Boolean.valueOf(mProperties.get(property));
    }
    return DEFAULT_BOOLEAN;
  }

  public synchronized int getIntProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Integer.valueOf(mProperties.get(property));
    }
    return DEFAULT_INTEGER;
  }

  public synchronized long getLongProperty(String property) {
    if (mProperties.containsKey(property)) {
      return Long.valueOf(mProperties.get(property));
    }
    return DEFAULT_LONG;
  }

  public synchronized String getProperty(String property) {
    if (mProperties.containsKey(property)) {
      return mProperties.get(property);
    }
    return DEFAULT_STRING;
  }
}
