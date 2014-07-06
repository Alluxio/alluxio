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
package tachyon.master;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Base object for all Json objects in Tachyon.
 */
abstract class JsonObject {
  /** Creates a JSON ObjectMapper configured not to close the underlying stream. */
  public static ObjectMapper createObjectMapper() {
    // TODO: Could disable field name quoting, though this would produce technically invalid JSON
    // See: JsonGenerator.QUOTE_FIELD_NAMES and JsonParser.ALLOW_UNQUOTED_FIELD_NAMES
    return new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false).configure(
        SerializationFeature.CLOSE_CLOSEABLE, false);
  }

  public Map<String, Object> parameters = Maps.newHashMap();

  /**
   * Generic parameter getter, useful for custom classes or enums.
   * Use a more specific getter, like getLong(), when available.
   */
  @SuppressWarnings("unchecked")
  public <T> T get(String name) {
    return (T) parameters.get(name);
  }

  public Boolean getBoolean(String name) {
    return this.get(name);
  }

  /** Deserializes a base64-encoded String as a ByteBuffer. */
  public ByteBuffer getByteBuffer(String name) {
    String byteString = get(name);
    if (byteString == null) {
      return null;
    }

    return ByteBuffer.wrap(Base64.decodeBase64(byteString));
  }

  /** Deserializes a list of base64-encoded Strings as a list of ByteBuffers. */
  public List<ByteBuffer> getByteBufferList(String name) {
    List<String> byteStrings = get(name);
    if (byteStrings == null) {
      return null;
    }

    List<ByteBuffer> buffers = Lists.newArrayListWithCapacity(byteStrings.size());
    for (String byteString : byteStrings) {
      buffers.add(ByteBuffer.wrap(Base64.decodeBase64(byteString)));
    }
    return buffers;
  }

  public Integer getInt(String name) {
    return this.<Number> get(name).intValue();
  }

  /**
   * Deserializes the parameter as a long.
   * Use of this function is necessary when dealing with longs, as they may
   * have been deserialized as integers if they were sufficiently small.
   */
  public Long getLong(String name) {
    return this.<Number> get(name).longValue();
  }

  public String getString(String name) {
    return this.get(name);
  }

  /** Adds the given named parameter to the Json object. Value must be JSON-serializable. */
  public JsonObject withParameter(String name, Object value) {
    parameters.put(name, value);
    return this;
  }
}
