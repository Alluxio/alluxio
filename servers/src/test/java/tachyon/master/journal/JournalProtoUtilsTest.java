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
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.protobuf.Message;
import com.google.protobuf.UnknownFieldSet;
import com.google.protobuf.UnknownFieldSet.Field;

import tachyon.exception.ExceptionMessage;
import tachyon.proto.JournalEntryProtos;
import tachyon.proto.JournalEntryProtos.AddMountPointEntry;
import tachyon.proto.JournalEntryProtos.JournalEntry;
import tachyon.proto.JournalEntryProtos.JournalEntry.EntryCase;

/**
 * Tests for {@link JournalUtils}.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(JournalEntryProtos.JournalEntry.class)
public final class JournalProtoUtilsTest {

  /**
   * Tests that getMessage returns the sub-message contained in a JournalEntry.
   */
  @Test
  public void getMessageTest() {
    AddMountPointEntry addMountEntry = AddMountPointEntry.newBuilder().build();
    tachyon.proto.JournalEntryProtos.JournalEntry entry =
        JournalEntryProtos.JournalEntry.newBuilder().setAddMountPoint(addMountEntry).build();
    Message message = JournalProtoUtils.getMessage(entry);
    Assert.assertTrue(message instanceof AddMountPointEntry);
    Assert.assertEquals(message, addMountEntry);
  }

  /**
   * Tests that the right exception is thrown when no sub-message is set for a JournalEntry.
   */
  @Test
  public void getNoMessageTest() {
    tachyon.proto.JournalEntryProtos.JournalEntry entry =
        JournalEntryProtos.JournalEntry.newBuilder().build();
    try {
      JournalProtoUtils.getMessage(entry);
      Assert.fail("getMessage() should fail when no messages is set");
    } catch (RuntimeException e) {
      Assert.assertEquals(ExceptionMessage.NO_ENTRY_TYPE.getMessage("[]"), e.getMessage());
    }
  }

  /**
   * Uses Powermock to test that unknown fields are reported when no journal sub-message is
   * recognized.
   */
  @Test
  public void getUnknownMessageTest() {
    JournalEntry unknownEntry = PowerMockito.mock(JournalEntryProtos.JournalEntry.class);
    Mockito.when(unknownEntry.getUnknownFields()).thenReturn(
        UnknownFieldSet.newBuilder().addField(46264, Field.newBuilder().build()).build());
    Mockito.when(unknownEntry.getEntryCase()).thenReturn(EntryCase.ENTRY_NOT_SET);
    try {
      JournalProtoUtils.getMessage(unknownEntry);
      Assert.fail("getMessage() should fail when no messages is set");
    } catch (RuntimeException e) {
      Assert.assertEquals(ExceptionMessage.NO_ENTRY_TYPE.getMessage("[46264]"), e.getMessage());
    }
  }
}
