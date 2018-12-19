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

import com.google.common.base.Objects;
import org.apache.commons.lang3.tuple.Triple;

import java.io.Serializable;
import java.util.List;
import java.util.TreeSet;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * Alluxio WebUI configuration information.
 */
@NotThreadSafe
public final class WebUIConfiguration implements Serializable {
  private TreeSet<Triple<String, String, String>> mConfiguration;
  private List<String> mWhitelist;

  /**
   * Creates a new instance of {@link WebUIConfiguration}.
   */
  public WebUIConfiguration() {
  }


  public TreeSet<Triple<String, String, String>> getConfiguration() {
    return mConfiguration;
  }

  public List<String> getWhitelist() {
    return mWhitelist;
  }

  public WebUIConfiguration setConfiguration(
      TreeSet<Triple<String, String, String>> configuration) {
    mConfiguration = configuration;
    return this;
  }

  public WebUIConfiguration setWhitelist(List<String> whitelist) {
    mWhitelist = whitelist;
    return this;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("mConfiguration", mConfiguration)
        .add("mWhitelist", mWhitelist).toString();
  }
}
