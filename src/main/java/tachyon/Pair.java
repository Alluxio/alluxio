package tachyon;

public class Pair<First, Second> {
  private First mFirst;
  private Second mSecond;

  public Pair(First first, Second second) {
    mFirst = first;
    mSecond = second;
  }

  public First getFirst() {
    return mFirst;
  }

  public Second getSecond() {
    return mSecond;
  }
}