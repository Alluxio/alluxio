package alluxio.client.file.cache.filter;

final class IndexAndTag {
    public final int index;
    public final int tag;

    IndexAndTag(int bucketIndex, int tag) {
        this.index = bucketIndex;
        this.tag = tag;
    }
}
