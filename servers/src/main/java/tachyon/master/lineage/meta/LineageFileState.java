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

package tachyon.master.lineage.meta;

import tachyon.exception.ExceptionMessage;

/**
 * The state of a lineage file.
 */
public enum LineageFileState {
  CREATED, COMPLETED, PERSISTED, PERSISENCE_REQUESTED, LOST;

  /**
   * @return the corresponding protobuf {@link tachyon.proto.journal.Lineage.LineageFileState}
   */
  public tachyon.proto.journal.Lineage.LineageFileState toJournalEntry() {
    switch (this) {
      case CREATED:
        return tachyon.proto.journal.Lineage.LineageFileState.CREATED;
      case COMPLETED:
        return tachyon.proto.journal.Lineage.LineageFileState.COMPLETED;
      case LOST:
        return tachyon.proto.journal.Lineage.LineageFileState.LOST;
      case PERSISENCE_REQUESTED:
        return tachyon.proto.journal.Lineage.LineageFileState.PERSISENCE_REQUESTED;
      case PERSISTED:
        return tachyon.proto.journal.Lineage.LineageFileState.PERSISTED;
      default:
        throw new IllegalStateException(
            ExceptionMessage.UNKNOWN_LINEAGE_FILE_STATE.getMessage(this.toString()));
    }
  }

  /**
   * @param state a protocol buffer lineage file state
   * @return the corresponding {@link LineageFileState} for the given protocol buffer
   */
  public static LineageFileState fromJournalEntry(
      tachyon.proto.journal.Lineage.LineageFileState state) {
    switch (state) {
      case CREATED:
        return CREATED;
      case COMPLETED:
        return COMPLETED;
      case LOST:
        return LOST;
      case PERSISENCE_REQUESTED:
        return PERSISENCE_REQUESTED;
      case PERSISTED:
        return PERSISTED;
      default:
        throw new IllegalStateException(
            ExceptionMessage.UNKNOWN_LINEAGE_FILE_STATE.getMessage(state.toString()));
    }
  }
}

