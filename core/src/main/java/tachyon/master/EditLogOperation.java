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
