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

package tachyon.master.next.filesystem.journal.json;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Objects;

import tachyon.master.next.journal.JournalEntryType;

/**
 * Represents an entry in the journal log file, which is serialized as Json. The entry is an event
 * happened in FileSystemMaster, like creating a file, deleting a file, etc. An event is composed of
 * a type, an id representing the time the event happens, and a set of parameters.
 */
public class EventEntry extends JsonObject {
  public JournalEntryType mType;
  public long mEventId;

  public EventEntry(JournalEntryType type, long eventId) {
    mType = type;
    mEventId = eventId;
  }

  /** Constructor used for deserializing the entry. */
  @JsonCreator
  public EventEntry(@JsonProperty("type") JournalEntryType type,
      @JsonProperty("eventId") long eventId,
      @JsonProperty("parameters") Map<String, JsonNode> parameters) {
    mType = type;
    mEventId = eventId;
    mParameters = parameters;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", mType).add("eventId", mEventId)
        .add("parameters", mParameters).toString();
  }

  @Override
  public EventEntry withParameter(String name, Object value) {
    return (EventEntry) super.withParameter(name, value);
  }
}
