package tachyon.master;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Objects;

/**
 * Each entry in the Image is represented as a single element, which is serialized as JSON. An
 * element has a type and a set of parameters determined by the type.
 */
class ImageElement extends JsonObject {
  // NB: These type names are used in the serialized JSON. They should be concise but readable.
  public ImageElementType mType;

  public ImageElement(ImageElementType type) {
    this.mType = type;
  }

  /** Constructor used for deserializing Elements. */
  @JsonCreator
  public ImageElement(@JsonProperty("type") ImageElementType type,
      @JsonProperty("parameters") Map<String, JsonNode> parameters) {
    mType = type;
    mParameters = parameters;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", mType)
        .add("parameters", mParameters).toString();
  }

  @Override
  public ImageElement withParameter(String name, Object value) {
    return (ImageElement) super.withParameter(name, value);
  }
}
