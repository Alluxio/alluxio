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

import alluxio.collections.Pair;
import alluxio.util.io.PathUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Versioned File Manager.
 */
public class VersionFileManager extends AbstractFileManager {
  public static final String FILE_PREFIX = "__MANAGED_FILE__";
  public static final String FILE_VERSION_PREFIX = "__VERSION__";

  /**
   * Construct a versioned file manager from a string root.
   * @param rootDir root directory
   * @param user User name
   * @param group Group Name
   */
  public VersionFileManager(String rootDir, String user, String group) {
    super(rootDir, user, group);
  }

  @Nullable
  private static Pair<String, Integer> parseFileName(String filename) {
    if (!filename.startsWith(FILE_PREFIX)) {
      return null;
    }
    filename = filename.replaceFirst(FILE_PREFIX, "");
    String[] nameParsed = filename.split(FILE_VERSION_PREFIX);
    if (nameParsed.length != 2) {
      return null;
    } else {
      try {
        return new Pair<>(nameParsed[0], Integer.parseInt(nameParsed[1]));
      } catch (NumberFormatException e) {
        return null;
      }
    }
  }

  private static String makeFileName(String filename, int version) {
    return FILE_PREFIX + filename + FILE_VERSION_PREFIX + version;
  }

  protected String getNextFilePath(String fileName) {
    Optional<Pair<File, Pair<String, Integer>>> result = Arrays.stream(mRootDir.listFiles())
        .map(x -> new Pair<>(x, parseFileName(x.getName())))
        .filter(x -> x.getSecond() != null)
        .filter(x -> x.getSecond().getFirst().equals(fileName))
        .max(Comparator.comparingInt(key -> key.getSecond().getSecond()));
    return PathUtils.concatPath(mRootDir,
        result.map(filePairPair -> makeFileName(fileName,
            filePairPair.getSecond().getSecond() + 1))
        .orElseGet(() -> makeFileName(fileName, 0)));
  }

  @Override
  public List<String> listFile() {
    return Arrays.stream(mRootDir.list())
        .map(VersionFileManager::parseFileName)
        .filter(Objects::nonNull)
        .map(Pair::getFirst)
        .distinct()
        .collect(Collectors.toList());
  }

  @Override
  public boolean removeFile(String fileName) {
    return Arrays.stream(mRootDir.listFiles())
        .map(x -> new Pair<>(x, parseFileName(x.getName())))
        .filter(x -> x.getSecond() != null)
        .filter(x -> x.getSecond().getFirst().equals(fileName))
        .allMatch(x -> x.getFirst().delete());
  }
}
