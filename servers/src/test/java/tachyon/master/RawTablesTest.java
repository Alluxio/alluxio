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

package tachyon.master;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.junit.Assert;
import org.junit.Test;

import tachyon.conf.TachyonConf;
import tachyon.thrift.TachyonException;

/**
 * Tests for tachyon.master.RawTables
 */
public class RawTablesTest {
  @Test
  public void writeImageTest() throws IOException, TachyonException {
    // crate the RawTables, byte buffers, and output streams
    RawTables rt = new RawTables(new TachyonConf());
    ByteBuffer bb1 = ByteBuffer.allocate(1);
    ByteBuffer bb2 = ByteBuffer.allocate(1);
    ByteBuffer bb3 = ByteBuffer.allocate(1);

    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);
    ObjectMapper mapper = JsonObject.createObjectMapper();
    ObjectWriter writer = mapper.writer();

    // add elements to the RawTables
    rt.addRawTable(0, 1, bb1);
    rt.addRawTable(1, 1, bb2);
    rt.addRawTable(2, 1, bb3);

    // write the image
    rt.writeImage(writer, dos);

    List<Integer> ids = Arrays.asList(0, 1, 2);
    List<Integer> columns = Arrays.asList(1, 1, 1);
    List<ByteBuffer> data = Arrays.asList(bb1, bb2, bb3);

    // decode the written bytes
    ImageElement decoded = mapper.readValue(os.toByteArray(), ImageElement.class);

    // test the decoded ImageElement
    Assert.assertEquals(ids, decoded.get("ids", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(columns, decoded.get("columns", new TypeReference<List<Integer>>() {}));
    Assert.assertEquals(data, decoded.get("data", new TypeReference<List<ByteBuffer>>() {}));
  }
}
