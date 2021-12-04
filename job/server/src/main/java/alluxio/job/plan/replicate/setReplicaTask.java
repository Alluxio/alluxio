package alluxio.job.plan.replicate;

import com.google.common.base.MoreObjects;

import java.io.Serializable;

/**
 * A task representing loading a block into the memory of a worker.
 */
public final class setReplicaTask implements Serializable {
    private static final long serialVersionUID = 2028545900913354425L;
    final Mode mMode;

    /**
     * @param mode evict or replicate
     */
    public setReplicaTask(Mode mode) {
        mMode = mode;
    }

    /**
     * @return the block id
     */
    public Mode getMode() {
        return mMode;
    }

    @Override public String toString() {
        return MoreObjects.toStringHelper(this).add("mode", mMode).toString();
    }
}
