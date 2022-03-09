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

import java.util.List;

/**
 * Intergace for a job worker log.
 */
public interface LogLink {

  /**
   * Get a link url for given job id.
   * @param id
   * @return a url link
   */
  String getLinkForChildJob(long id);

  /**
   * Add a link.
   * @param id
   * @param url
   */
  void addLinkForChildJob(long id, String url);

  /**
   * get a loglink for a job worker.
   * @return log link string
   */
  List<String> getAllLogLinks();
}
