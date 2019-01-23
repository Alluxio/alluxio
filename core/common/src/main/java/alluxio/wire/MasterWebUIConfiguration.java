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

package alluxio.wire;

import com.google.common.base.MoreObjects;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.util.List;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI configuration information.
 */
@NotThreadSafe
public final class MasterWebUIConfiguration implements Serializable {
  private static final long serialVersionUID = -2277858633604882055L;

  private List<String> mWhitelist;
  private TreeSet<Triple<String, String, String>> mConfiguration;

  /**
   * Creates a new instance of {@link MasterWebUIConfiguration}.
   */
  public MasterWebUIConfiguration() {
  }

  /**
   * Gets configuration.
   *
   * @return the configuration
   */
  public TreeSet<Triple<String, String, String>> getConfiguration() {
    return mConfiguration;
  }

  /**
   * Gets whitelist.
   *
   * @return the whitelist
   */
  public List<String> getWhitelist() {
    return mWhitelist;
  }

  /**
   * Sets configuration.
   *
   * @param configuration the configuration
   * @return the configuration
   */
  public MasterWebUIConfiguration setConfiguration(
      TreeSet<Triple<String, String, String>> configuration) {
    mConfiguration = configuration;
    return this;
  }

  /**
   * Sets whitelist.
   *
   * @param whitelist the whitelist
   * @return the whitelist
   */
  public MasterWebUIConfiguration setWhitelist(List<String> whitelist) {
    mWhitelist = whitelist;
    return this;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("configuration", mConfiguration)
        .add("whitelist", mWhitelist).toString();
  }
}
