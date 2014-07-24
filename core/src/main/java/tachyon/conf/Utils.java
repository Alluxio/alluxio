/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.conf;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

import tachyon.util.CommonUtils;

/**
 * Utils for tachyon.conf package.
 */
class Utils {
  private static final Logger LOG = Logger.getLogger("");

  protected static Configuration hadoopConf;
  protected static PropertiesConfiguration commonConf;

  static {
    commonConf = new PropertiesConfiguration();
    try {
      /* add tachyon.properties. if not over-ridden then loaded from JAR */
      commonConf.load(Configuration.class.getClassLoader().getResourceAsStream(
          "tachyon.properties"));
    } catch (ConfigurationException e) {
      LOG.error("Fatal error loading tachyon.properties:" + e);
    }
    setConf(new Configuration(false));
  }

  public static void setConf(Configuration conf) {
    hadoopConf = conf;
    /* add default XML values stored in the JAR */
    hadoopConf.addResource(Configuration.class.getClassLoader().getResourceAsStream(
        "tachyon-site.xml"));
    /* add any over-ride tachyon-site.xml from the classpath/class loader */
    hadoopConf.addResource("tachyon-site.xml");
    hadoopConf.reloadConfiguration();
  }

  public static boolean getBooleanProperty(String property) {
    return Boolean.valueOf(getProperty(property));
  }

  public static boolean getBooleanProperty(String property, boolean defaultValue) {
    return Boolean.valueOf(getProperty(property, defaultValue + ""));
  }

  public static int getIntProperty(String property) {
    return Integer.valueOf(getProperty(property));
  }

  public static int getIntProperty(String property, int defaultValue) {
    return Integer.valueOf(getProperty(property, defaultValue + ""));
  }

  public static long getLongProperty(String property) {
    return Long.valueOf(getProperty(property));
  }

  public static long getLongProperty(String property, int defaultValue) {
    return Long.valueOf(getProperty(property, defaultValue + ""));
  }

  public static String getProperty(String property) {
    /*
     * TODO: Remove all System.setProperty(..) and System.getProperty(..) calls which
     * breaks o.a.CommonsConfig system property handling. Instead use a shared
     * configuration object.
     */
    String ret = System.getProperty(property);
    if (ret == null) {
      ret = commonConf.getString(property);
    }
    if (ret == null)
      ret = hadoopConf.get(property);
    
    if (ret == null) {
      CommonUtils.illegalArgumentException(property + " is not configured.");
    }
    LOG.debug(property + " : " + ret);
    return ret;
  }

  public static String getProperty(String property, String defaultValue) {
    String ret = null;
    String msg = "";
    try {
      ret = getProperty(property);
    } catch (IllegalArgumentException ex) {
      ret = defaultValue;
      msg = " uses the default value";
    }
    LOG.debug(property + msg + " : " + ret);
    return ret;
  }
  public static void setProperty(String key, String value) {
    String ret = System.getProperty(key);
    if (ret != null) {
      System.setProperty(key, value);
    }else{
      ret = commonConf.getString(key);
    }
    
    if(ret!=null){
      commonConf.setProperty(key, value);
    }else{
      ret = hadoopConf.get(key);
    }
    
    if(ret!=null){
      hadoopConf.set(key, value);
    }else{
      //default spot for unset properties.  can be changed later.
      System.setProperty(key, value);
    }

  }
}
