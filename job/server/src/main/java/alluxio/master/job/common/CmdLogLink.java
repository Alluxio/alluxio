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

package alluxio.master.job.common;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Command loglink class.
 */
public class CmdLogLink implements LogLink {
  private final Map<Long, String> mMap;
  private final long mJobControlId;

  /**
   * Constructor.
   * @param id
   */
  public CmdLogLink(long id) {
    mJobControlId = id;
    mMap = Maps.newHashMap();
  }

  /**
   * Get the log link for a child job.
   * @param id
   * @return a string log url
   */
  @Override
  public String getLinkForChildJob(long id) {
    return mMap.getOrDefault(id, "");
  }

  /**
   * Add a string url to the map.
   * @param id
   * @param url
   */
  @Override
  public void addLinkForChildJob(long id, String url) {
    mMap.put(id, url);
  }

  /**
   * Get the job control id.
   * @return job control id
   */
  public long getControlId() {
    return mJobControlId;
  }

  /**
   * Get all child job url links.
   * @return a list of string of all links
   */
  @Override
  public List<String> getAllLogLinks() {
    return new ArrayList<>(mMap.values());
  }
}
