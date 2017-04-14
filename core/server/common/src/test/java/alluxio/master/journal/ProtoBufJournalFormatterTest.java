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

package alluxio.master.journal;

import alluxio.proto.journal.File.CompleteFileEntry;
import alluxio.proto.journal.Journal.JournalEntry;

import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

/**
 * Unit tests for {@link ProtoBufJournalFormatter}.
 */
public final class ProtoBufJournalFormatterTest extends AbstractJournalFormatterTest {
  @Override
  protected JournalFormatter getFormatter() {
    return new ProtoBufJournalFormatter();
  }

  @Test
  public void truncatedEntryDoesntFail() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    ProtoBufJournalFormatter formatter = new ProtoBufJournalFormatter();
    formatter.serialize(JournalEntry.newBuilder().setCompleteFile(
        CompleteFileEntry.newBuilder().setId(10)).build(), baos);
    byte[] serializedEntry = baos.toByteArray();
    for (int i = 1; i < serializedEntry.length; i++) {
      byte[] truncated = ArrayUtils.subarray(serializedEntry, 0, serializedEntry.length - i);
      Assert.assertEquals(null,
          formatter.deserialize(new ByteArrayInputStream(truncated)).read());
    }
  }
}
