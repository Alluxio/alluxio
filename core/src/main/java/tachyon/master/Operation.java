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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Each entry in the EditLog is represented as a single Operation, which is serialized as JSON.
 * An Operation has a type, a transaction id, and a set of parameters determined by the type.
 */
class Operation {
  // NB: These type names are used in the serialized JSON. They should be concise but readable.
  public OperationType type;
  public long transId;
  public Map<String, Object> parameters = Maps.newHashMap();

  public Operation(OperationType type, long transId) {
    this.type = type;
    this.transId = transId;
  }

  /** Constructor used for deserializing Operations. */
  @JsonCreator
  public Operation(@JsonProperty("type") OperationType type,
      @JsonProperty("transId") long transId,
      @JsonProperty("parameters") Map<String, Object> parameters) {
    this.type = type;
    this.transId = transId;
    this.parameters = parameters;
  }

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

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", type).add("transId", transId)
        .add("parameters", parameters).toString();
  }

  /** Adds the given named parameter to the Operation. Value must be JSON-serializable. */
  public Operation withParameter(String name, Object value) {
    parameters.put(name, value);
    return this;
  }

}