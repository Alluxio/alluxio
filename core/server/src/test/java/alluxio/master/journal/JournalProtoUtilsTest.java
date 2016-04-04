/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.journal;

import alluxio.exception.ExceptionMessage;
import alluxio.proto.journal.File.AddMountPointEntry;
import alluxio.proto.journal.Journal.JournalEntry;

import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;
import org.junit.Assert;
import org.junit.Test;
/**
 * Unit tests for {@link JournalProtoUtils}.
 */
public final class JournalProtoUtilsTest {

  /**
   * Tests that getMessage returns the sub-message contained in a JournalEntry.
   */
  @Test
  public void getMessageTest() {
    AddMountPointEntry addMountEntry = AddMountPointEntry.newBuilder().build();
    JournalEntry entry = JournalEntry.newBuilder()
        .setAddMountPoint(addMountEntry)
        .build();
    Message message = JournalProtoUtils.unwrap(entry);
    Assert.assertTrue(message instanceof AddMountPointEntry);
    Assert.assertEquals(message, addMountEntry);
  }

  /**
   * Tests that the right exception is thrown when no sub-message is set for a JournalEntry.
   */
  @Test
  public void getUnknownMessageTest() {
    JournalEntry unknownEntry = JournalEntry.newBuilder().build();
    unknownEntry = unknownEntry.toBuilder()
        .setUnknownFields(
            UnknownFieldSet.newBuilder().addField(46264, Field.newBuilder().build()).build())
        .build();
    try {
      JournalProtoUtils.unwrap(unknownEntry);
      Assert.fail("getMessage() should fail when no messages is set");
    } catch (RuntimeException e) {
      Assert.assertEquals(ExceptionMessage.NO_ENTRY_TYPE.getMessage("[46264]"), e.getMessage());
    }
  }
}
