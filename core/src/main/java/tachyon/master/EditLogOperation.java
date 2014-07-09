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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Each entry in the EditLog is represented as a single Operation, which is serialized as JSON.
 * An Operation has a type, a transaction id, and a set of parameters determined by the type.
 */
class EditLogOperation extends JsonObject {
  // NB: These type names are used in the serialized JSON. They should be concise but readable.
  public EditLogOperationType type;
  public long transId;

  public EditLogOperation(EditLogOperationType type, long transId) {
    this.type = type;
    this.transId = transId;
  }

  /** Constructor used for deserializing Operations. */
  @JsonCreator
  public EditLogOperation(@JsonProperty("type") EditLogOperationType type,
      @JsonProperty("transId") long transId,
      @JsonProperty("parameters") Map<String, Object> parameters) {
    this.type = type;
    this.transId = transId;
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", type).add("transId", transId)
        .add("parameters", parameters).toString();
  }

  @Override
  public EditLogOperation withParameter(String name, Object value) {
    return (EditLogOperation) super.withParameter(name, value);
  }
}