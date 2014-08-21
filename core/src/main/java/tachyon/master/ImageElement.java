package tachyon.master;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Each entry in the Image is represented as a single element, which is serialized as JSON.
 * An element has a type and a set of parameters determined by the type.
 */
class ImageElement extends JsonObject {
  // NB: These type names are used in the serialized JSON. They should be concise but readable.
  public ImageElementType type;

  public ImageElement(ImageElementType type) {
    this.type = type;
  }

  /** Constructor used for deserializing Elements. */
  @JsonCreator
  public ImageElement(@JsonProperty("type") ImageElementType type,
      @JsonProperty("parameters") Map<String, Object> parameters) {
    this.type = type;
    this.parameters = parameters;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("type", type).add("parameters", parameters).toString();
  }

  @Override
  public ImageElement withParameter(String name, Object value) {
    return (ImageElement) super.withParameter(name, value);
  }
}
