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

import java.io.File;

import com.typesafe.config.*;

class UtilsFile implements UtilsBase {
  Config mConf = null;

  public UtilsFile() {
    mConf = ConfigFactory.load();
  }

  public UtilsFile(String name) {
    File file = new File(name);
    mConf = ConfigFactory.parseFile(file);
  }

  @Override
  public boolean getBooleanProperty(String property) {
    return mConf.getBoolean(property);
  }

  @Override
  public boolean getBooleanProperty(String property, boolean defaultValue) {
    try {
      return mConf.getBoolean(property);
    }
    catch (ConfigException.Missing e){
      return defaultValue;
    }
  }

  @Override
  public int getIntProperty(String property) {
    return mConf.getInt(property);
  }

  @Override
  public int getIntProperty(String property, int defaultValue) {
    try {
      return mConf.getInt(property);
    }
    catch (ConfigException.Missing e){
      return defaultValue;
    }
  }

  @Override
  public long getLongProperty(String property) {
    return mConf.getLong(property);
  }

  @Override
  public long getLongProperty(String property, int defaultValue) {
    try {
      return mConf.getLong(property);
    }
    catch (ConfigException.Missing e){
      return defaultValue;
    }
  }

  @Override
  public String getProperty(String property) {
    return mConf.getString(property);
  }

  @Override
  public String getProperty(String property, String defaultValue) {
    try {
      return mConf.getString(property);
    }
    catch (ConfigException.Missing e){
      return defaultValue;
    }
  }
}
