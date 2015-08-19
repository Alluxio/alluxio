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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.fasterxml.jackson.databind.ObjectWriter;

import tachyon.master.next.filesystem.journal.AddCheckpointEvent;
import tachyon.master.next.filesystem.journal.EventReadWriter;
import tachyon.master.next.journal.EventType;

public class JsonEventReadWriter extends EventReadWriter {
  private ObjectWriter mWriter = JsonObject.createObjectMapper().writer();
  private long mTransactionId = 0;

  public JsonEventReadWriter() {
    // TODO(cc)
  }

  @Override
  protected void writeAddCheckPointEvent(AddCheckpointEvent event, OutputStream os)
      throws IOException {
    JsonEvent jsonEvent =
        new JsonEvent(EventType.ADD_CHECKPOINT, ++mTransactionId)
            .withParameter("fileId", event.fileId()).withParameter("length", event.length())
            .withParameter("path", event.checkpointPath().toString())
            .withParameter("opTimeMs", event.opTimeMs());

    writeJsonEvent(jsonEvent, os);
  }

  private void writeJsonEvent(JsonEvent event, OutputStream os) throws IOException {
    mWriter.writeValue(os, event);
    (new DataOutputStream(os)).writeByte('\n');
  }
}
