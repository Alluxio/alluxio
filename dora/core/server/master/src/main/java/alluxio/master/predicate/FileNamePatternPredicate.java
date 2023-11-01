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

package alluxio.master.predicate;

import alluxio.proto.journal.Job.FileFilter;
import alluxio.underfs.UfsStatus;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * A predicate related to date of the file.
 */
public class FileNamePatternPredicate implements FilePredicate {
  private static final Logger LOG = LoggerFactory.getLogger(FileNamePatternPredicate.class);
  private final String mFilterName;
  private String mRegexPatternStr;

  /**
   * Factory for modification time predicate.
   */
  public static class FileNamePatternFactory extends Factory {
    @Override
    public String getFilterName() {
      return "fileNamePattern";
    }
  }

  /**
   * Factory for creating instances.
   */
  public abstract static class Factory implements FilePredicateFactory {
    /**
     * @return filter name for the predicate
     */
    public abstract String getFilterName();

    /**
     * Creates a {@link FilePredicate} from the string value.
     *
     * @param regexPatternStr the regex pattern string from the filter
     * @return the created predicate
     */
    public FilePredicate createFileNamePatternPredicate(String regexPatternStr) {
      return new FileNamePatternPredicate(getFilterName(), regexPatternStr);
    }

    @Override
    public FilePredicate create(FileFilter filter) {
      try {
        if (filter.hasName() && filter.getName().equals(getFilterName())) {
          if (filter.hasValue()) {
            return createFileNamePatternPredicate(filter.getValue());
          }
        }
      } catch (Exception e) {
        // fall through
      }
      return null;
    }
  }

  /**
   * Creates an instance.
   *
   * @param filterName the filter name
   * @param regexPatternStr the regex pattern string for file filtering
   */
  public FileNamePatternPredicate(String filterName, String regexPatternStr) {
    mFilterName = filterName;
    mRegexPatternStr = regexPatternStr;
  }

  @Override
  public Predicate<FileInfo> get() {
    return FileInfo -> {
      try {
        String fileName = FileInfo.getName();
        return Pattern.matches(mRegexPatternStr, fileName);
      } catch (RuntimeException e) {
        LOG.debug("Failed to filter: ", e);
        return false;
      }
    };
  }

  @Override
  public Predicate<UfsStatus> getUfsStatusPredicate() {
    return UfsStatus -> {
      try {
        String fileName = getFileName(UfsStatus);
        return Pattern.matches(mRegexPatternStr, fileName);
      } catch (RuntimeException e) {
        LOG.debug("Failed to filter: ", e);
        return false;
      }
    };
  }

  private String getFileName(UfsStatus ufsStatus) {
    String name = ufsStatus.getName();
    int index = name.lastIndexOf("/");
    if (index == -1) {
      return name;
    }
    name = name.substring(index + 1);
    return name;
  }
}
