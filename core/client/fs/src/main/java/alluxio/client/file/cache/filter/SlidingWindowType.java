package alluxio.client.file.cache.filter;

public enum SlidingWindowType {
    COUNT_BASED(0),
    TIME_BASED(1),
    NONE(2);

    public final int type;

    private SlidingWindowType(int type) {
        this.type = type;
    }
}
