package alluxio.util.webui;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The type Ui metric.
 */
public class UITriple implements Comparable<UITriple> {
  private String mLeft;
  private String mMiddle;
  private String mRight;

  /**
   * Instantiates a new Ui metric.
   *
   * @param left the left
   * @param middle the middle
   * @param right the right
   */
  @JsonCreator
  public UITriple(@JsonProperty("left") String left, @JsonProperty("middle") String middle,
      @JsonProperty("right") String right) {
    mLeft = left;
    mMiddle = middle;
    mRight = right;
  }

  /**
   * Gets left.
   *
   * @return the left
   */
  public String getLeft() {
    return mLeft;
  }

  /**
   * Gets middle.
   *
   * @return the middle
   */
  public String getMiddle() {
    return mMiddle;
  }

  /**
   * Gets right.
   *
   * @return the right
   */
  public String getRight() {
    return mRight;
  }

  /**
   * Sets left.
   *
   * @param left the left
   */
  public void setLeft(String left) {
    mLeft = left;
  }

  /**
   * Sets middle.
   *
   * @param middle the middle
   */
  public void setMiddle(String middle) {
    mMiddle = middle;
  }

  /**
   * Sets right.
   *
   * @param right the right
   */
  public void setRight(String right) {
    mRight = right;
  }

  @Override
  public int compareTo(UITriple o) {
    return this.mLeft.compareTo(o.mLeft);
  }
}
