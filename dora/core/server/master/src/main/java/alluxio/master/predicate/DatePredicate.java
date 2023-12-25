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

import alluxio.exception.runtime.InvalidArgumentRuntimeException;
import alluxio.master.predicate.interval.Interval;
import alluxio.proto.journal.Job.FileFilter;
import alluxio.underfs.UfsStatus;
import alluxio.wire.FileInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A predicate related to date of the file.
 */
public class DatePredicate implements FilePredicate {
  private static final Logger LOG = LoggerFactory.getLogger(DatePredicate.class);
  private final String mFilterName;
  private String mValue;
  private final Interval mInterval;
  private final Function<FileInfo, Long> mGetter;

  /**
   * Factory for modification time predicate.
   */
  public static class LastModifiedDateFactory extends Factory {
    @Override
    public String getFilterName() {
      return "lastModifiedDate";
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
    public FilePredicate createDatePredicate(String value) {
      return new DatePredicate(getFilterName(), value, getTimestampGetter());
    }

    @Override
    public FilePredicate create(FileFilter filter) {
      try {
        if (filter.hasName() && filter.getName().equals(getFilterName())) {
          if (filter.hasValue()) {
            return createDatePredicate(filter.getValue());
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
   *              one date like "2020/03/01" to define an interval after a start date, or
   *              two dates delimited by comma like "2000/01/01, 2020/09/01" to define an interval
   *              between a start date and an end date
   * @param getter a getter function that returns the timestamp to compare against
   */
  public DatePredicate(String filterName, String value, Function<FileInfo, Long> getter) {
    mFilterName = filterName;
    mValue = value;
    mInterval = parseInterval(value);
    mGetter = getter;
  }

  private Interval parseInterval(String stringValue) {
    String[] values = stringValue.split(",");
    if (values.length == 1) {
      return Interval.after(convertDateToMs(values[0].trim()));
    }
    if (values.length == 2) {
      return Interval.between(
          convertDateToMs(values[0].trim()), convertDateToMs(values[1].trim()));
    }
    throw new InvalidArgumentRuntimeException("Fail to parse " + stringValue);
  }

  private long convertDateToMs(String dateString) {
    SimpleDateFormat f = new SimpleDateFormat("yyyy/MM/dd");
    try {
      Date d = f.parse(dateString);
      return d.getTime();
    } catch (ParseException e) {
      throw new InvalidArgumentRuntimeException(e);
    }
  }

  @Override
  public Predicate<FileInfo> get() {
    return FileInfo -> {
      try {
        Long time = mGetter.apply(FileInfo);
        Interval interval = Interval.between(time, time + 1);
        return mInterval.intersect(interval).isValid();
      } catch (RuntimeException e) {
        LOG.debug("Failed to filter: ", e);
        return false;
      }
    };
  }

  @Override
  public Predicate<UfsStatus> getUfsStatusPredicate() {
    throw new UnsupportedOperationException(
        "getUfsStatusPredicate() is unsupported in DatePredicate");
  }
}
