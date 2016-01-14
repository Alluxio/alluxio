/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package tachyon.master.journal;

import org.junit.Assert;
import org.junit.Test;

import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;

import tachyon.exception.ExceptionMessage;
import tachyon.proto.journal.File.AddMountPointEntry;
import tachyon.proto.journal.Journal.JournalEntry;
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
