/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.wire;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

public class MissingRequiredFieldsParsingException extends ProtoParsingException {
  protected MissingRequiredFieldsParsingException(Throwable cause) {
    super(cause);
  }

  public MissingRequiredFieldsParsingException(
      Descriptors.FieldDescriptor missingField, Message protoMessage) {
    super(String.format("Field %s is required, but is missing in the proto message: %s",
        missingField.getName(), protoMessage.toString()));
  }
}
