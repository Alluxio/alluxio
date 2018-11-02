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

package alluxio.master.journal.options;

import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

/**
 * Unit tests for {@link JournalWriterOptions}.
 */
public final class JournalWriterOptionsTest {
  @Test
  public void defaults() {
    JournalWriterOptions options = JournalWriterOptions.defaults();
    Assert.assertEquals(0, options.getNextSequenceNumber());
    Assert.assertEquals(false, options.isPrimary());
  }

  /**
   * Tests getting and setting fields.
   */
  @Test
  public void fields() {
    Random random = new Random();
    boolean isPrimary = random.nextBoolean();
    long sequenceNumber = random.nextLong();
    JournalWriterOptions options = JournalWriterOptions.defaults();
    options.setPrimary(isPrimary);
    Assert.assertEquals(isPrimary, options.isPrimary());
    options.setNextSequenceNumber(sequenceNumber);
    Assert.assertEquals(sequenceNumber, options.getNextSequenceNumber());
  }
}
