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

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Unit tests for {@link TimePredicate}.
 */
public class TimePredicateTest {
  /**
   * The exception expected to be thrown.
   */
  @Rule
  public final ExpectedException mThrown = ExpectedException.none();

  @Test
  public void testTimePredicateFactories() {
    FileFilter filter = FileFilter.newBuilder().setName("unmodifiedFor").setValue("1s").build();
    long timestamp = System.currentTimeMillis();
    FileInfo info = new FileInfo();
    info.setLastModificationTimeMs(timestamp);

    assertFalse(FilePredicate.create(filter).get().test(info));

    info.setLastModificationTimeMs(timestamp - 1000);
    assertTrue(FilePredicate.create(filter).get().test(info));
  }

  @Test
  public void testCreateEmptyPredicate() {
    FileFilter filter = FileFilter.newBuilder().setName("").setValue("").build();
    long timestamp = System.currentTimeMillis();
    FileInfo info = new FileInfo();
    info.setLastModificationTimeMs(timestamp);
    mThrown.expect(UnsupportedOperationException.class);
    mThrown.expectMessage("Invalid filter name: ");
    FilePredicate.create(filter).get().test(info);
  }

  @Test
  public void testDateFromFileNameOlderThan() {
    FileFilter filter = FileFilter.newBuilder().setName("dateFromFileNameOlderThan").setValue("2d")
        .setPattern("YYYYMMDD").build();
    FileInfo info = new FileInfo();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMdd");
    LocalDateTime now = LocalDateTime.now();
    LocalDateTime threeDaysBefore = now.minusDays(3);
    String date = dtf.format(threeDaysBefore);
    info.setName(date);

    assertTrue(FilePredicate.create(filter).get().test(info));

    LocalDateTime oneDayBefore = now.minusDays(1);
    date = dtf.format(oneDayBefore);
    info.setName(date);
    assertFalse(FilePredicate.create(filter).get().test(info));
  }
}

