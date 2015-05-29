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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
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

  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  public Map<String, JsonNode> mParameters = Maps.newHashMap();

  /**
   * Generic parameter getter, useful for custom classes or enums. Use a more specific getter, like
   * getLong(), when available.
   */
  public <T> T get(String name, Class<T> clazz) {
    return OBJECT_MAPPER.convertValue(mParameters.get(name), clazz);
  }

  /**
   * Get the value for parameterized type class such as <code>List<Integer>/code>
   * using the help of <code>TypeReference</code>
   */
  public <T> T get(String name, TypeReference<T> typeReference) {
    return OBJECT_MAPPER.convertValue(mParameters.get(name), typeReference);
  }

  public Boolean getBoolean(String name) {
    return this.get(name, Boolean.class);
  }

  /** Deserializes a base64-encoded String as a ByteBuffer. */
  public ByteBuffer getByteBuffer(String name) {
    String byteString = get(name, String.class);
    if (byteString == null) {
      return null;
    }

    return ByteBuffer.wrap(Base64.decodeBase64(byteString));
  }

  /** Deserializes a list of base64-encoded Strings as a list of ByteBuffers. */
  public List<ByteBuffer> getByteBufferList(String name) {
    List<String> byteStrings = get(name, new TypeReference<List<String>>() {});
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
    return this.get(name, Number.class).intValue();
  }

  /**
   * Deserializes the parameter as a long. Use of this function is necessary when dealing with
   * longs, as they may have been deserialized as integers if they were sufficiently small.
   */
  public Long getLong(String name) {
    return this.get(name, Number.class).longValue();
  }

  /**
   * Deserializes the parameter as a short. Use of this function is necessary when dealing with
   * shorts such as short permission (eg. 00777)
   */
  public Short getShort(String name) {
    return this.get(name, Number.class).shortValue();
  }

  public String getString(String name) {
    return this.get(name, String.class);
  }

  /** Adds the given named parameter to the Json object. Value must be JSON-serializable. */
  public JsonObject withParameter(String name, Object value) {
    mParameters.put(name, OBJECT_MAPPER.convertValue(value, JsonNode.class));
    return this;
  }
}
