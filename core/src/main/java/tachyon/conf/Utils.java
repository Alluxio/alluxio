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

import org.apache.log4j.Logger;

/**
 * Utils class to retrieve configuration from system properties and configuration files.
 */
public class Utils {
  UtilsOpt  mSys = null;
  UtilsFile mFile = null;
  String    mResource = null;

  public Utils(){
    mSys = new UtilsOpt();
  }

  public Utils(String name){
    LOG.info("use configure file " + name);
    mResource = name;
    mFile = new UtilsFile(name);
    mSys = new UtilsOpt();
  }

  public void addResource(String name){
    LOG.info("use configure file " + name);
    mResource = name;
    if (mFile == null){
      mFile = new UtilsFile(name);
    }
  }

  private final Logger LOG = Logger.getLogger("");

  public boolean getBooleanProperty(String property) {
    try {
      // attempt to get property from system first
      return mSys.getBooleanProperty(property);
    }
    catch (IllegalArgumentException e) {
      // if property is not found, try config file
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getBooleanProperty(property);
      }
      throw e;
    }
  }

  public boolean getBooleanProperty(String property, boolean defaultValue) {
    try {
      return mSys.getBooleanProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getBooleanProperty(property, defaultValue);
      }
      return defaultValue;
    }
  }

  public int getIntProperty(String property) {
    try {
      return mSys.getIntProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getIntProperty(property);
      }
      throw e;
    }    
  }

  public int getIntProperty(String property, int defaultValue) {
    try {
      return mSys.getIntProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getIntProperty(property, defaultValue);
      }
      return defaultValue;
    }        
  }

  public long getLongProperty(String property) {
    try {
      return mSys.getLongProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getLongProperty(property);
      }
      throw e;
    }    
  }

  public long getLongProperty(String property, int defaultValue) {
    try {
      return mSys.getLongProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getLongProperty(property);
      }
      return defaultValue;
    }        
  }

  public String getProperty(String property) {
    try {
      return mSys.getProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getProperty(property);
      }
      throw e;
    }        
  }

  public String getProperty(String property, String defaultValue) {
    try {
      return mSys.getProperty(property);
    }
    catch (IllegalArgumentException e) {
      if (mFile != null) {
        LOG.debug("search " + mResource + " for property " + property);
        return mFile.getProperty(property, defaultValue);
      }
      return defaultValue;
    }        
  }
}
