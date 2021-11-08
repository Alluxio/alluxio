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

package alluxio.hub.agent.util.file;

import java.util.List;

/**
 * This interface defines a file manager interface.
 */
public interface FileManager {
  /**
   * Add a file to the file manager.
   *
   * @param fileName filename
   * @param permission permission string
   * @param content content
   *
   * @return true if successfully added
   */
  boolean addFile(String fileName, String permission, byte[] content);

  /**
   * List all the files in a file manager.
   *
   * @return a list of file names
   */
  List<String> listFile();

  /**
   * Remove a file.
   *
   * @param fileName file name
   * @return true if successfully removed the file
   */
  boolean removeFile(String fileName);
}
