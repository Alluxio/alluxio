package tachyon;

/**
 * A pair representation defined by two elements (of type First and type Second).
 * 
 * @param <T1> the first element of the Pair.
 * @param <T2> the second element of the Pair.
 */
public final class Pair<T1, T2> {
  private T1 mFirst;
  private T2 mSecond;

  /**
   * Constructs and initializes a Pair specified by the two input elements.
   * 
   * @param first the first element of the pair (of type First)
   * @param second the second element of the pair (of type Second)
   */
  public Pair(T1 first, T2 second) {
    mFirst = first;
    mSecond = second;
  }

  /**
   * @return the first element of the pair.
   */
  public T1 getFirst() {
    return mFirst;
  }

  /**
   * @return the second element of the pair.
   */
  public T2 getSecond() {
    return mSecond;
  }

  /**
   * Set the first value.
   * 
   * @param first the value to be set.
   */
  public void setFirst(T1 first) {
    mFirst = first;
  }

  /**
   * Set the second value.
   * 
   * @param second the value to be set.
   */
  public void setSecond(T2 second) {
    mSecond = second;
  }

  @Override
  public String toString() {
    return new StringBuilder().append("Pair(").append(mFirst).append(",").append(mSecond)
        .append(")").toString();
  }
}
