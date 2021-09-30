package alluxio.client.file.cache.filter;

enum CuckooStatus {
  OK(0), FAILURE(1), FAILURE_KEY_NOT_FOUND(2), FAILURE_KEY_DUPLICATED(3), FAILURE_TABLE_FULL(
      4), UNDEFINED(5);

  public int code;

  CuckooStatus(int code) {
    this.code = code;
  }
}


public class TagPosition {
  public int bucketIndex;
  public int tagIndex;
  public CuckooStatus status;

  public TagPosition() {
    this(-1, -1, CuckooStatus.UNDEFINED);
  }

  public TagPosition(int bucketIndex, int tagIndex) {
    this(bucketIndex, tagIndex, CuckooStatus.UNDEFINED);
  }

  public TagPosition(int bucketIndex, int tagIndex, CuckooStatus status) {
    this.bucketIndex = bucketIndex;
    this.tagIndex = tagIndex;
    this.status = status;
  }

  boolean valid() {
    return bucketIndex >= 0 && tagIndex >= 0;
  }

  public int getBucketIndex() {
    return bucketIndex;
  }

  public void setBucketIndex(int bucketIndex) {
    this.bucketIndex = bucketIndex;
  }

  public int getTagIndex() {
    return tagIndex;
  }

  public void setTagIndex(int tagIndex) {
    this.tagIndex = tagIndex;
  }

  public CuckooStatus getStatus() {
    return status;
  }

  public void setStatus(CuckooStatus status) {
    this.status = status;
  }

  public void setBucketAndSlot(int bucket, int slot) {
    this.bucketIndex = bucket;
    this.tagIndex = slot;
  }

  @Override
  public String toString() {
    return "TagPosition{" + "bucketIndex=" + bucketIndex + ", tagIndex=" + tagIndex + '}';
  }
}
