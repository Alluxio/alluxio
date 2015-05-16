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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit Test for EditLogOperation
 */
public class EditLogOperationTest {
  private static final String CREATE_DEPENDENCY_TYPE = "{\"type\":\"CREATE_DEPENDENCY\","
      + "\"parameters\":{\"parents\":[1,2,3],\"commandPrefix\":\"fake command\","
      + "\"dependencyId\":1,\"frameworkVersion\":\"0.3\",\"data\":[\"AAAAAAAAAAAAAA==\"],"
      + "\"children\":[4,5,6,7],"
      + "\"comment\":\"Comment Test\",\"creationTimeMs\":1409349750338,"
      + "\"dependencyType\":\"Narrow\",\"framework\":\"Tachyon Examples\"}}";

  private static final ObjectMapper OBJECT_MAPPER = JsonObject.createObjectMapper();

  // Tests for CREATE_DEPENDENCY operation
  @Test
  public void createDependencyTest() throws IOException {
    EditLogOperation editLogOperation =
        OBJECT_MAPPER.readValue(CREATE_DEPENDENCY_TYPE.getBytes(), EditLogOperation.class);

    // get all parameters for "CREATE_DEPENDENCY"
    List<Integer> parents = editLogOperation.get("parents", new TypeReference<List<Integer>>() {});
    Assert.assertEquals(3, parents.size());

    List<Integer> children =
        editLogOperation.get("children", new TypeReference<List<Integer>>() {});
    Assert.assertEquals(4, children.size());

    String commandPrefix = editLogOperation.getString("commandPrefix");
    Assert.assertEquals("fake command", commandPrefix);

    List<ByteBuffer> data = editLogOperation.getByteBufferList("data");
    Assert.assertEquals(1, data.size());
    String decodedBase64 = new String(data.get(0).array(), "UTF-8");
    Assert.assertEquals(new String(Base64.decodeBase64("AAAAAAAAAAAAAA==")), decodedBase64);

    String comment = editLogOperation.getString("comment");
    Assert.assertEquals("Comment Test", comment);

    String framework = editLogOperation.getString("framework");
    Assert.assertEquals("Tachyon Examples", framework);

    String frameworkVersion = editLogOperation.getString("frameworkVersion");
    Assert.assertEquals("0.3", frameworkVersion);

    DependencyType dependencyType = editLogOperation.get("dependencyType", DependencyType.class);
    Assert.assertEquals(DependencyType.Narrow, dependencyType);

    Integer depId = editLogOperation.getInt("dependencyId");
    Assert.assertEquals(1, depId.intValue());

    Long creationTimeMs = editLogOperation.getLong("creationTimeMs");
    Assert.assertEquals(1409349750338L, creationTimeMs.longValue());
  }

}
