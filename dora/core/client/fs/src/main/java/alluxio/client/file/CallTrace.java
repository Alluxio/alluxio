package alluxio.client.file;

public class CallTrace {
    private final long mPosition;
    private final int mLength;

    CallTrace(long pos, int length) {
        mPosition = pos;
        mLength = length;
    }
    public long getPosition() {
        return mPosition;
    }
    public int getLength() {
        return mLength;
    }
}