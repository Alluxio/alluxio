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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

class UtilsFile implements UtilsBase {
  Configuration mConf = null;

  public UtilsFile() {
    if (mConf == null) {
      mConf = new Configuration();
    }
    // default conf
    addResource("tachyon.xml");
  }

  public UtilsFile(String name) {
    if (mConf == null) {
      mConf = new Configuration();
    }
    mConf.addResource(new Path(name));
  }

  public void addResource(String name) {
    if (mConf == null){
      mConf = new Configuration();
    }
    mConf.addResource(new Path(name));
  }

  @Override
  public boolean getBooleanProperty(String property) {
    return mConf.getBoolean(property, false);
  }

  @Override
  public boolean getBooleanProperty(String property, boolean defaultValue) {
    return mConf.getBoolean(property, defaultValue);
  }

  @Override
  public int getIntProperty(String property) {
    return mConf.getInt(property, -1);
  }

  @Override
  public int getIntProperty(String property, int defaultValue) {
    return mConf.getInt(property, defaultValue);
  }

  @Override
  public long getLongProperty(String property) {
    return mConf.getLong(property, -1);
  }

  @Override
  public long getLongProperty(String property, int defaultValue) {
    return mConf.getLong(property, defaultValue);
  }

  @Override
  public String getProperty(String property) {
    return mConf.get(property);
  }

  @Override
  public String getProperty(String property, String defaultValue) {
    return mConf.get(property, defaultValue);
  }

}
