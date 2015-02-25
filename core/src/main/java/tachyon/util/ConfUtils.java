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

package tachyon.util;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tachyon.Constants;
import tachyon.conf.TachyonConf;

/**
 * Utility class for {@link tachyon.conf.TachyonConf}
 */
public class ConfUtils {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Prevent instantiation
   */
  private ConfUtils() {}

  /**
   * Store the source {@link tachyon.conf.TachyonConf} object to the target
   * Hadoop {@link org.apache.hadoop.conf.Configuration} object.
   *
   * @param source the {@link tachyon.conf.TachyonConf} to be stored
   * @param target the {@link org.apache.hadoop.conf.Configuration} target
   */
  public static void storeToHadoopConfiguration(TachyonConf source, Configuration target) {
    // Need to set io.serializations key to prevent NPE when trying to get SerializationFactory.
    target.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,"
        + "org.apache.hadoop.io.serializer.WritableSerialization");
    Properties confProperties = source.getInternalProperties();
    try {
      DefaultStringifier.store(target, confProperties, Constants.TACHYON_CONF_SITE);
    } catch (IOException ex) {
      LOG.error("Unable to store TachyonConf in Haddop configuration", ex);
      throw new RuntimeException(ex);
    }
  }

  /**
   * Load {@link TachyonConf} from Hadoop {@link org.apache.hadoop.conf.Configuration} source
   * @param source the {@link org.apache.hadoop.conf.Configuration} to load from.
   * @return instance of {@link TachyonConf} to be loaded
   */
  public static TachyonConf loadFromHadoopConfiguration(Configuration source) {
    // Load TachyonConf if any and merge to the one in TachyonFS
    // Push TachyonConf to the Job conf
    if (source.get(Constants.TACHYON_CONF_SITE) != null) {
      LOG.info("Found TachyonConf site from Job configuration for Tachyon");
      Properties tachyonConfProperties = null;
      try {
        tachyonConfProperties = DefaultStringifier.load(source, Constants.TACHYON_CONF_SITE,
            Properties.class);
      } catch (IOException ex) {
        LOG.error("Unable to load TachyonConf from Haddop configuration", ex);
        throw new RuntimeException(ex);
      }
      if (tachyonConfProperties != null) {
        return new TachyonConf(tachyonConfProperties);
      } else {
        return null;
      }
    } else {
      return null;
    }
  }
}
