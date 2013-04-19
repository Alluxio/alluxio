package tachyon;

/**
 * A pair representation defined by two elements (of type First and type Second).
 * 
 * @param <First> the first element of the Pair. 
 * @param <Second> the second element of the Pair.
 */
public class Pair<First, Second> {
  private First mFirst;
  private Second mSecond;

  /**
   * Constructs and initializes a Pair specified by the two input elements.
   * @param first the first element of the pair (of type First)
   * @param second the second element of the pair (of type Second)
   */
  public Pair(First first, Second second) {
    mFirst = first;
    mSecond = second;
  }

  /**
   * @return the first element of the pair.
   */
  public First getFirst() {
    return mFirst;
  }

  /**
   * @return the second element of the pair.
   */
  public Second getSecond() {
    return mSecond;
  }
}