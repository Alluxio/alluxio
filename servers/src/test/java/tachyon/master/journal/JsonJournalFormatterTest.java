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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JsonJournalFormatterTest extends JournalFormatterTestBase {

  public static final String JSON_SAMPLE_PATH = "JsonJournalEntries.json";

  private JsonNode mRootNode;

  @Override
  protected JournalFormatter getFormatter() {
    return new JsonJournalFormatter();
  }

  @Override
  @Before
  public void before() throws Exception {
    String entriesFile = JsonJournalFormatterTest.class.getResource(JSON_SAMPLE_PATH).getFile();
    mRootNode = new ObjectMapper().readTree(new File(entriesFile));
    super.before();
  }

  @Test
  public void entriesJsonTest() throws IOException {
    ObjectMapper om = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
        .configure(SerializationFeature.CLOSE_CLOSEABLE, false);

    for (Map.Entry<JournalEntryType, JournalEntry> entry : mDataSet.entrySet()) {
      JournalEntryType type = entry.getKey();
      JsonNode n = mRootNode.get(type.toString());
      om.writeValue(mOs, n);
      JournalEntry readEntry = read();
      assertSameEntry(entry.getValue(), readEntry);
    }
  }
}
