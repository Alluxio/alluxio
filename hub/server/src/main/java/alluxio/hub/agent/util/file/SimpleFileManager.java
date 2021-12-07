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

import alluxio.exception.AlluxioException;
import alluxio.util.io.PathUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Simple File Manager.
 */
public class SimpleFileManager extends AbstractFileManager {
  /**
   * Construct a simple file manager from a root.
   * @param rootDir root directory
   * @param user User name
   * @param group Group Name
   */
  public SimpleFileManager(String rootDir, String user, String group) {
    super(rootDir, user, group);
  }

  @Override
  protected String getNextFilePath(String fileName) throws InvalidPathException {
    try {
      verifyFileName(fileName);
    } catch (AlluxioException e) {
      throw new InvalidPathException(fileName, e.getMessage());
    }
    return PathUtils.concatPath(mRootDir, fileName);
  }

  @Override
  public List<String> listFile() {
    return mRootDir.list() == null ? Collections.EMPTY_LIST
        : Arrays.stream(mRootDir.list()).collect(Collectors.toList());
  }

  @Override
  public boolean removeFile(String fileName) {
    try {
      return Files.list(mRootDir.toPath())
          .filter(x -> Optional.ofNullable(x.getFileName())
              .map(Object::toString).orElse("").equals(fileName))
          .allMatch(path -> {
            try {
              return Files.deleteIfExists(path);
            } catch (IOException e) {
              return false;
            }
          });
    } catch (IOException e) {
      return false;
    }
  }
}
