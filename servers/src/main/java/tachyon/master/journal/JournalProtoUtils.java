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

import com.google.protobuf.Message;

import tachyon.exception.ExceptionMessage;
import tachyon.proto.JournalEntryProtos.JournalEntry;

/**
 * Utils for working with the journal.
 */
public final class JournalProtoUtils {

  public static Message getMessage(JournalEntry entry) {
    switch (entry.getEntryCase()) {
      case ADDMOUNTPOINT:
        return entry.getAddMountPoint();
      case ENTRY_NOT_SET:
        // This could mean that the field was never set, or it was set with a different version of
        // this message. Given the history of the JournalEntry protobuf message, the keys of the
        // unknown fields should be enough to figure out which version of JournalEntry is needed to
        // understand this journal.
        throw new RuntimeException(
            ExceptionMessage.NO_ENTRY_TYPE.getMessage(entry.getUnknownFields().asMap().keySet()));
      default:
        throw new IllegalStateException(
            ExceptionMessage.UNKNOWN_ENTRY_TYPE.getMessage(entry.getEntryCase()));
    }
  }

  private JournalProtoUtils() {} // not for instantiation
}
