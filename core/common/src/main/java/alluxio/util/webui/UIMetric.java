package alluxio.util.webui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The Ui metric.
 */
public class UIMetric {
  private long mValue;

  /**
   * Instantiates a new Ui metric.
   *
   * @param value the value
   */
  @JsonCreator
  public UIMetric(@JsonProperty("value") long value) {
    mValue = value;
  }

  /**
   * @param value the value
   */
  public void setValue(long value) {
    mValue = value;
  }

  /**
   * @return the value
   */
  public long getValue() {
    return mValue;
  }
}
