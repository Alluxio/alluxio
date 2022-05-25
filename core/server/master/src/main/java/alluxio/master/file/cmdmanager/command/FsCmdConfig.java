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

package alluxio.master.file.cmdmanager.command;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Interface for filesystem command config.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "@type")
public interface FsCmdConfig {
  /**
   * @return name of the fs Cmd
   */
  String getName();

//  /**
//   * @return JobSource of the fs Cmd
//   */
//  JobSource getJobSource();

  /**
   * @return type of the fs Cmd
   */
  CmdType getCmdType();

  /**
   * @return source file path of the fs Cmd
   */
  String getSrcFilePath();

  /**
   * Get bandwidth value from the user.
   * @return bandwidth
   */
  Bandwidth getBandWidth();
}
