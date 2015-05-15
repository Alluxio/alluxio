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

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Objects;

/**
 * Each entry in EditLog is represented as a single EditLogOperation, which is serialized as a JSON
 * object. An EditLogOperation has a type, a transaction id, and a set of parameters according to
 * the type.
 */
class EditLogOperation extends JsonObject {
  // NB: These type names are used in the serialized JSON. They should be concise but readable.
  public EditLogOperationType mType;
  public long mTransId;

  public EditLogOperation(EditLogOperationType type, long transId) {
    mType = type;
    mTransId = transId;
  }

  /** Constructor used for deserializing Operations. */
  @JsonCreator
  public EditLogOperation(@JsonProperty("type") EditLogOperationType type,
      @JsonProperty("transId") long transId,
      @JsonProperty("parameters") Map<String, JsonNode> parameters) {
    mType = type;
    mTransId = transId;
    mParameters = parameters;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", mType).add("transId", mTransId)
        .add("parameters", mParameters).toString();
  }

  @Override
  public EditLogOperation withParameter(String name, Object value) {
    return (EditLogOperation) super.withParameter(name, value);
  }
}
