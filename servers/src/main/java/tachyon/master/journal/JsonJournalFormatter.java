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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;

import tachyon.Constants;

public final class JsonJournalFormatter implements JournalFormatter {

  /** Creates a JSON ObjectMapper configured not to close the underlying stream. */
  private ObjectMapper createObjectMapper() {
    // TODO(cc): Could disable field name quoting, though this would produce technically invalid
    // JSON. See: JsonGenerator.QUOTE_FIELD_NAMES and JsonParser.ALLOW_UNQUOTED_FIELD_NAMES
    return new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(SerializationFeature.CLOSE_CLOSEABLE, false);
  }

  private final ObjectMapper mObjectMapper = createObjectMapper();
  private final ObjectWriter mObjectWriter = mObjectMapper.writer();

  @Override
  public void serialize(SerializableJournalEntry entry, OutputStream outputStream)
      throws IOException {
    writeEntry(entry, outputStream);
  }

  private void writeEntry(SerializableJournalEntry entry, OutputStream os) throws IOException {
    mObjectWriter.writeValue(os, entry);
    (new DataOutputStream(os)).write('\n');
  }

  @Override
  public JournalInputStream deserialize(final InputStream inputStream) throws IOException {
    return new JournalInputStream() {
      private JsonParser mParser = mObjectMapper.getFactory().createParser(inputStream);
      private long mLatestSequenceNumber = 0;

      @Override
      public JournalEntry getNextEntry() throws IOException {
        JsonNode entryNode;
        try {
          entryNode = mParser.readValueAs(JsonNode.class);
        } catch (JsonProcessingException e) {
          return null;
        }
        mLatestSequenceNumber =
            entryNode.get(Constants.JOURNAL_JSON_ENTRY_SEQUENCE_NUMBER_KEY).asLong();
        JsonNode parametersNode = entryNode.get(Constants.JOURNAL_JSON_ENTRY_PARAMETER_KEY);
        String entryTypeStr = entryNode.get(Constants.JOURNAL_JSON_ENTRY_TYPE_KEY).asText();

        JournalEntryType entryType;
        try {
          entryType = JournalEntryType.valueOf(entryTypeStr);
        } catch (IllegalArgumentException e) {
          throw new IOException("Unknown journal entry type: " + entryTypeStr);
        }
        return mObjectMapper.convertValue(parametersNode, entryType.getClazz());
      }

      @Override
      public void close() throws IOException {
        inputStream.close();
      }

      @Override
      public long getLatestSequenceNumber() {
        return mLatestSequenceNumber;
      }
    };
  }
}
