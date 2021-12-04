package alluxio.job.plan.replicate;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

/**
 * A task representing loading a block into the memory of a worker.
 */
public final class setReplicaCommand implements Serializable {
    private static final long serialVersionUID = 2028545900913354425L;
    final Enum mMode;

    /**
     * @param mode evict or replicate
     */
    public setReplicaCommand(Enum mode) {
        mMode = mode;
    }

    /**
     * @return the block id
     */
    public Enum getMode() {
        return mMode;
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this).add("mode", mMode).toString();
    }
}
