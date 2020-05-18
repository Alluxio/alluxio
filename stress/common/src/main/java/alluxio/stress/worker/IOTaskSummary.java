package alluxio.stress.worker;

import alluxio.stress.GraphGenerator;
import alluxio.stress.JsonSerializable;
import alluxio.stress.Summary;
import alluxio.stress.job.IOConfig;
import alluxio.stress.master.MasterBenchParameters;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class IOTaskSummary implements Summary {
    private List<IOTaskResult.Point> mPoints;
    private List<String> mErrors;

    // TODO(jiacheng)
    private WorkerBenchParameters mParameters;
    private List<String> mNodes;

    private SpeedStat mReadSpeedStat;

    public SpeedStat getReadSpeedStat() {
        return mReadSpeedStat;
    }

    public SpeedStat getWriteSpeedStat() {
        return mWriteSpeedStat;
    }

    private SpeedStat mWriteSpeedStat;

//    private int mTotalReadSize;
//    private long mTotalReadDuration;
//    private double mReadDurationStdDev;
//    private double mMaxReadSpeed;
//    private double mMinReadSpeed;
//    private double mAvgReadSpeed;
//    private double mReadSpeedStdDev;
//    private int mTotalWriteSize;
//    private long mTotalWriteDuration;
//    private double mWriteDurationStdDev;
//    private double mMaxWriteSpeed;
//    private double mMinWriteSpeed;
//    private double mAvgWriteSpeed;
//    private double mWriteSpeedStdDev;

    public IOTaskSummary(IOTaskResult result) {
        mPoints = new ArrayList<>(result.getPoints());
        mErrors = new ArrayList<>(result.getErrors());

        calculateStats();
    }

    public static class SpeedStat implements JsonSerializable {
        public long mTotalDuration;
        public int mTotalSize;
        public double mMaxSpeed;
        public double mMinSpeed;
        public double mAvgSpeed;
        public double mStdDev;

        public SpeedStat() {
            mTotalDuration = 0L;
            mTotalSize = 0;
            mMaxSpeed = 0.0;
            mMinSpeed = 0.0;
            mAvgSpeed = 0.0;
            mStdDev = 0.0;
        }
    }

    /**
     * The points must be valid (duration not equal to 0).
     */
    public static SpeedStat calculateStdDev(List<IOTaskResult.Point> points) {
        SpeedStat result = new SpeedStat();
        if (points.size() == 0) {
            return result;
        }

        long totalDuration = 0L;
        int totalSize = 0;
        double[] speeds = new double[points.size()];
        double maxSpeed = 0.0;
        double minSpeed = Double.MAX_VALUE;
        int i = 0;
        for (IOTaskResult.Point p : points) {
            totalDuration += p.mDurationMs;
            totalSize += p.mDataSizeMB;
            double speed = p.mDataSizeMB / (double) p.mDurationMs;
            maxSpeed = Math.max(maxSpeed, speed);
            minSpeed = Math.min(minSpeed, speed);
            speeds[i++] = p.mDataSizeMB / (double) p.mDurationMs;
        }
        double avgSpeed = totalSize / (double) totalDuration;
        double var=0;
        for (int j = 0; j < speeds.length; j++) {
            var += (speeds[j] - avgSpeed) * (speeds[j] - avgSpeed);
        }

        result.mTotalDuration = totalDuration;
        result.mTotalSize = totalSize;
        result.mMaxSpeed = maxSpeed;
        result.mMinSpeed = Double.compare(minSpeed, Double.MAX_VALUE) == 0 ? 0.0 : minSpeed;
        result.mAvgSpeed = avgSpeed;
        result.mStdDev = Math.sqrt(var);

        return result;
    }

    private void calculateStats() {
        List<IOTaskResult.Point> readPoints = mPoints.stream().filter((p) ->
                p.mMode == IOConfig.IOMode.READ && p.mDurationMs > 0)
                .collect(Collectors.toList());
        mReadSpeedStat = calculateStdDev(readPoints);

        List<IOTaskResult.Point> writePoints = mPoints.stream().filter((p) ->
                p.mMode == IOConfig.IOMode.WRITE && p.mDurationMs > 0)
                .collect(Collectors.toList());
        mWriteSpeedStat = calculateStdDev(writePoints);


//        mMinReadSpeed = readStat.mMinSpeed;
//        mMaxReadSpeed = readStat.mMaxSpeed;
//        mAvgReadSpeed = readStat.mAvgSpeed;
//        mTotalReadSize = readStat.mTotalSize;
//        mTotalReadDuration = readStat.mTotalDuration;
//        mReadSpeedStdDev = readStat.mStdDev;
//
//        mMinWriteSpeed = writeStat.mMinSpeed;
//        mMaxWriteSpeed = writeStat.mMaxSpeed;
//        mAvgWriteSpeed = writeStat.mAvgSpeed;
//        mTotalWriteSize = writeStat.mTotalSize;
//        mTotalWriteDuration = writeStat.mTotalDuration;
//        mWriteSpeedStdDev = writeStat.mStdDev;
    }

    public List<IOTaskResult.Point> getPoints() {
        return mPoints;
    }

    public List<String> getErrors() {
        return mErrors;
    }

    public void setErrors(List<String> errors) {
        mErrors = errors;
    }

    public void setPoints(List<IOTaskResult.Point> points) {
        mPoints = points;
    }

    @Override
    public GraphGenerator graphGenerator() {
        // TODO(jiacheng): what is a graph???
        return null;
    }

    @Override
    public String toString() {
        return String.format("IOTaskSummary: {Points={}, Errors={}}",
                mPoints, mErrors);
    }
}
