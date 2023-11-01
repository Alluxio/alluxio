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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.proto.journal.Job.FileFilter;
import alluxio.wire.FileInfo;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Unit tests for {@link DatePredicate}.
 */
public class DatePredicateTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();
  private SimpleDateFormat mDateFormat = new SimpleDateFormat("yyyy/MM/dd");

  @Test
  public void testDatePredicateFactories() throws ParseException {
    FileFilter filter =
        FileFilter.newBuilder().setName("lastModifiedDate").setValue("2020/01/01").build();
    long timestamp = System.currentTimeMillis();
    FileInfo info = new FileInfo();
    info.setLastModificationTimeMs(timestamp);

    assertTrue(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2019/09/05").getTime());
    assertFalse(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2020/01/01").getTime());
    assertTrue(FilePredicate.create(filter).get().test(info));
  }

  @Test
  public void testDatePredicateInterval() throws ParseException {

    FileFilter filter =
        FileFilter.newBuilder().setName("lastModifiedDate").setValue("2020/01/01, 2023/09/14")
                  .build();
    FileInfo info = new FileInfo();
    info.setLastModificationTimeMs(mDateFormat.parse("2021/09/15").getTime());
    assertTrue(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2019/09/05").getTime());
    assertFalse(FilePredicate.create(filter).get().test(info));

    Date d = mDateFormat.parse("2023/09/16");
    info.setLastModificationTimeMs(d.getTime());
    assertFalse(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2020/01/01").getTime());
    assertTrue(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2023/09/14").getTime() - 1);
    assertTrue(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2023/09/13").getTime());
    assertTrue(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2023/09/14").getTime());
    assertFalse(FilePredicate.create(filter).get().test(info));
    info.setLastModificationTimeMs(mDateFormat.parse("2023/09/14").getTime() + 1);
    assertFalse(FilePredicate.create(filter).get().test(info));
  }

  @Test
  public void testInvalidDateFormat() throws ParseException {
    FileFilter filter =
        FileFilter.newBuilder().setName("lastModifiedDate").setValue("2020-01-01, 2023-09-14")
                  .build();
    FileInfo info = new FileInfo();
    info.setLastModificationTimeMs(mDateFormat.parse("2021/09/15").getTime());

    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Invalid filter name: ");

    FilePredicate.create(filter).get().test(info);
  }

  @Test
  public void testInvalidDateString() throws ParseException {
    FileFilter filter =
        FileFilter.newBuilder().setName("lastModifiedDate").setValue("2020/01/01, 2023/09/14")
                  .build();
    FileInfo info = new FileInfo();

    mThrown.expect(ParseException.class);
    mThrown.expectMessage("Unparseable date: \"2021-09-15\"");
    info.setLastModificationTimeMs(mDateFormat.parse("2021-09-15").getTime());
    FilePredicate.create(filter).get().test(info);
  }
}
