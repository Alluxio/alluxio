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

import alluxio.exception.runtime.AlluxioRuntimeException;
import alluxio.master.predicate.interval.Interval;
import alluxio.proto.journal.Job.FileFilter;
import alluxio.util.FormatUtils;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.GregorianCalendar;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A predicate related to timestamps of the path.
 */
public class TimePredicate implements FilePredicate {
  private static final Logger LOG = LoggerFactory.getLogger(TimePredicate.class);
  private final String mFilterName;
  private String mValue;
  private final Interval mInterval;
  private final Function<FileInfo, Long> mGetter;

  /**
   * Factory for file name time predicate.
   * This predicate needs a pattern from the user input to extract the date from the filename.
   * For now, we support exact date format i.e. YYYY-MM-DD, /bin/usr/fileYYYY-MM-DD.
   * YYYY represents the number of year. MM represents the number of month. DD represents the
   * number of day. Later we will support more different patterns.
   * For example, if the pattern is aaaaYYYYMMDD, it will extract date from aaaa20230304,
   * aaaa2023030588999, bbbb20230304.
   * A warning message will be thrown if the pattern is empty, or it does not include YYYY,
   * MM and DD.
   */
  public static class DateFromFileNameOlderThanFactory implements FilePredicateFactory {
    /**
     * @return filter name
     */
    public String getFilterName() {
      return "dateFromFileNameOlderThan";
    }

    @Override
    public FilePredicate create(FileFilter filter) {
      try {
        if (filter.hasName() && filter.getName().equals(getFilterName())) {
          if (filter.hasPattern()) {
            String pattern = filter.getPattern();
            if (pattern.contains("YYYY") && pattern.contains("MM") && pattern.contains("DD")) {
              return new TimePredicate(getFilterName(), filter.getValue(),
                  FileInfo -> findTimestampFromFileName(FileInfo, pattern));
            }
          }
        }
      } catch (Exception e) {
        // fall through
      }
      return null;
    }

    /**
     * Extract the timestamp from the file name based on the pattern.
     *
     * @param fileInfo the file info
     * @param pattern the file name pattern
     * @return the timestamp extracted from the file name
     * @throws AlluxioRuntimeException this function would throw exceptions when errors
     * happen during extracting timestamps.
     */
    private static Long findTimestampFromFileName(FileInfo fileInfo, String pattern) {
      try {
        String fileName = fileInfo.getName();

        // We will use exact date match pattern for now, will add more
        // different types of pattern later.
        int dateIndex = pattern.indexOf("DD");
        int date = Integer.parseInt(fileName.substring(dateIndex, dateIndex + 2));

        int monthIndex = pattern.indexOf("MM");
        int month = Integer.parseInt(fileName.substring(monthIndex, monthIndex + 2));

        int yearIndex = pattern.indexOf("YYYY");
        int year = Integer.parseInt(fileName.substring(yearIndex, yearIndex + 4));

        GregorianCalendar fileNameDate = new GregorianCalendar(year, month - 1, date);

        return fileNameDate.getTimeInMillis();
      } catch (RuntimeException e) {
        throw AlluxioRuntimeException.from(e);
      }
    }
  }

  /**
   * Factory for modification time predicate.
   */
  public static class UnmodifiedForFactory extends Factory {
    @Override
    public String getFilterName() {
      return "unmodifiedFor";
    }

    @Override
    public Function<FileInfo, Long> getTimestampGetter() {
      return FileInfo::getLastModificationTimeMs;
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
     * @return getter for the timestamp
     */
    public abstract Function<FileInfo, Long> getTimestampGetter();

    /**
     * Creates a {@link FilePredicate} from the string value.
     *
     * @param value the value from the filter
     * @return the created predicate
     */
    public FilePredicate createTimePredicate(String value) {
      return new TimePredicate(getFilterName(), value, getTimestampGetter());
    }

    @Override
    public FilePredicate create(FileFilter filter) {
      try {
        if (filter.hasName() && filter.getName().equals(getFilterName())) {
          if (filter.hasValue()) {
            return createTimePredicate(filter.getValue());
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
   * @param value the string representation of the time span. The string should contain either
   *              one timestamp like "31h" to define an interval after a start time, or
   *              two timestamps delimited by comma like "15m, 1d" to define an interval
   *              between a start time and an end time
   * @param getter a getter function that returns the timestamp to compare against
   */
  public TimePredicate(String filterName, String value, Function<FileInfo, Long> getter) {
    mFilterName = filterName;
    mValue = value;
    mInterval = parseInterval(value);
    mGetter = getter;
  }

  private Interval parseInterval(String stringValue) {
    String[] values = stringValue.split(",");
    if (values.length == 1) {
      return Interval.after(FormatUtils.parseTimeSize(values[0].trim()));
    }
    if (values.length == 2) {
      return Interval.between(
          FormatUtils.parseTimeSize(values[0].trim()), FormatUtils.parseTimeSize(values[1].trim()));
    }
    throw new IllegalArgumentException("Fail to parse " + stringValue);
  }

  @Override
  public Predicate<FileInfo> get() {
    long currentTimeMS = System.currentTimeMillis();
    Interval interval = Interval.between(currentTimeMS, currentTimeMS + 1);
    return FileInfo -> {
      try {
        return interval.intersect(mInterval.add(mGetter.apply(FileInfo))).isValid();
      } catch (RuntimeException e) {
        LOG.debug("Failed to filter: ", e);
        return false;
      }
    };
  }
}
