package alluxio.stress.worker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = WorkerBenchStatsDeserializer.class)
public class WorkerBenchStats {

    @JsonProperty("workerID")
    public String mWorkerID;
    @JsonProperty("threadID")
    public long mThreadID;

    @JsonProperty("duration")
    public long mDuration;
    @JsonProperty("start")
    public long mStart;
    @JsonProperty("ioBytes")
    public long mIOBytes;

    @JsonCreator
    public WorkerBenchStats(@JsonProperty("workerID") String workerID,
                            @JsonProperty("threadID") long threadID,
                            @JsonProperty("start") long start,
                            @JsonProperty("duration") long duration,
                            @JsonProperty("ioBytes") long ioBytes) {
        mWorkerID = workerID;
        mThreadID = threadID;
        mStart = start;
        mDuration = duration;
        mIOBytes = ioBytes;
    }

    public String getWorkerID() {
        return mWorkerID;
    }

    public long getThreadID() {
        return mThreadID;
    }

    public long getDuration() {
        return mDuration;
    }

    public long getStart() {
        return mStart;
    }

    public long getmIOBytes(){
        return mIOBytes;
    }

    public void setWorkerID(String workerID) {
        mWorkerID = workerID;
    }

    public void setThreadID(long threadID) {
        mThreadID = threadID;
    }

    public void setDuration(long duration) {
        mDuration = duration;
    }

    public void setStart(long start) {
        mStart = start;
    }

    public void setIOBytes(long ioBytes) {
        mIOBytes = ioBytes;
    }
}
