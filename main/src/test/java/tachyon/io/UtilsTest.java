/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tachyon.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import tachyon.TestUtils;

public class UtilsTest {

  @Test
  public void writeReadByteBufferTest() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);

    List<ByteBuffer> bufs = new ArrayList<ByteBuffer>();
    bufs.add(null);
    bufs.add(ByteBuffer.allocate(0));
    bufs.add(TestUtils.getIncreasingByteBuffer(99));
    bufs.add(TestUtils.getIncreasingByteBuffer(10, 99));
    bufs.add(null);

    for (int k = 0; k < bufs.size(); k ++) {
      Utils.writeByteBuffer(bufs.get(k), dos);
    }
    ByteBuffer buf = TestUtils.getIncreasingByteBuffer(10, 99);
    buf.get();
    Utils.writeByteBuffer(buf, dos);

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

    for (int k = 0; k < bufs.size(); k ++) {
      Assert.assertEquals(bufs.get(k), Utils.readByteBuffer(dis));
    }
    Assert.assertEquals(buf, Utils.readByteBuffer(dis));

    dos.close();
    dis.close();
  }

  @Test
  public void writeReadStringTest() throws IOException {
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(os);

    List<String> strings = new ArrayList<String>();
    strings.add("");
    strings.add(null);
    strings.add("abc xyz");
    strings.add("123 789");
    strings.add("!@#$%^&*()_+}{\":?><");

    for (int k = 0; k < strings.size(); k ++) {
      Utils.writeString(strings.get(k), dos);
    }

    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(os.toByteArray()));

    for (int k = 0; k < strings.size(); k ++) {
      Assert.assertEquals(strings.get(k), Utils.readString(dis));
    }

    dos.close();
    dis.close();
  }
}