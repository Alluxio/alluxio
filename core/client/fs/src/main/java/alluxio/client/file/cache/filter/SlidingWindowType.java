package alluxio.client.file.cache.filter;

public enum SlidingWindowType {
    COUNT_BASED(0),
    TIME_BASED(1),
    NONE(3);

    public final int type;

    private SlidingWindowType(int type) {
        this.type = type;
    }
}
