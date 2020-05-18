package alluxio.stress.worker;

import alluxio.stress.GraphGenerator;
import alluxio.stress.JsonSerializable;
import alluxio.stress.Summary;
import alluxio.stress.job.IOConfig;
import alluxio.stress.master.MasterBenchParameters;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
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

    public IOTaskSummary(IOTaskResult result) {
        mPoints = new ArrayList<>(result.getPoints());
        mErrors = new ArrayList<>(result.getErrors());

        calculateStats();
    }

    @JsonSerialize(using = StatSerializer.class)
    @JsonDeserialize(using = StatDeserializer.class)
    public static class SpeedStat implements JsonSerializable {
        public double mTotalDuration; // in second
        public int mTotalSize; // in MB
        public double mMaxSpeed; // in MB/s
        public double mMinSpeed; // in MB/s
        public double mAvgSpeed; // in MB/s
        public double mStdDev; // in MB/s

        public SpeedStat() {
            mTotalDuration = 0.0;
            mTotalSize = 0;
            mMaxSpeed = 0.0;
            mMinSpeed = 0.0;
            mAvgSpeed = 0.0;
            mStdDev = 0.0;
        }

        @Override
        public String toString() {
            return String.format("{totalDuration=%s, totalSize=%s, maxSpeed=%s, minSpeed=%s, " +
                    "avgSpeed=%s, stdDev=%s}", mTotalDuration, mTotalSize, mMaxSpeed,
                    mMinSpeed, mAvgSpeed, mStdDev);
        }
    }

    public static class StatSerializer extends StdSerializer<SpeedStat> {
        public StatSerializer() {
            super(SpeedStat.class);
        }

        @Override
        public void serialize(SpeedStat value, JsonGenerator jgen, SerializerProvider provider)
                throws IOException, JsonProcessingException {
            jgen.writeStartObject();
            jgen.writeStringField("totalDuration", value.mTotalDuration + "s");
            jgen.writeStringField("totalSize", value.mTotalSize + "MB");
            jgen.writeStringField("maxSpeed", value.mMaxSpeed + "MB/s");
            jgen.writeStringField("minSpeed", value.mMinSpeed + "MB/s");
            jgen.writeStringField("avgSpeed", value.mAvgSpeed + "MB/s");
            jgen.writeNumberField("stdDev", value.mStdDev);
            jgen.writeEndObject();
        }
    }

    public static class StatDeserializer extends StdDeserializer<SpeedStat> {
        public StatDeserializer(){
            super(SpeedStat.class);
        }

        private double speedToNumber(String speed) {
            return Double.parseDouble(speed.substring(0, speed.length() - "MB/s".length()));
        }

        private double timeToNumber(String time) {
            return Double.parseDouble(time.substring(0, time.length() - "s".length()));
        }

        private int sizeToNumber(String size) {
            return Integer.parseInt(size.substring(0, size.length() - "MB".length()));
        }

        @Override
        public SpeedStat deserialize(JsonParser jp, DeserializationContext ctxt)
                throws IOException, JsonProcessingException {
            JsonNode node = jp.getCodec().readTree(jp);
            SpeedStat stat = new SpeedStat();
            stat.mTotalDuration = timeToNumber(node.get("totalDuration").asText());
            stat.mTotalSize = sizeToNumber(node.get("totalSize").asText());
            stat.mMaxSpeed = speedToNumber(node.get("maxSpeed").asText());
            stat.mMinSpeed = speedToNumber(node.get("minSpeed").asText());
            stat.mAvgSpeed = speedToNumber(node.get("avgSpeed").asText());
            stat.mStdDev = node.get("stdDev").asDouble();

            return stat;
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
            double speed = 1000 * p.mDataSizeMB / (double) p.mDurationMs; // Convert from MB/ms to MB/s
            maxSpeed = Math.max(maxSpeed, speed);
            minSpeed = Math.min(minSpeed, speed);
            speeds[i++] = p.mDataSizeMB / (double) p.mDurationMs;
        }
        double avgSpeed = totalSize / (double) totalDuration;
        double var=0;
        for (int j = 0; j < speeds.length; j++) {
            var += (speeds[j] - avgSpeed) * (speeds[j] - avgSpeed);
        }

        result.mTotalDuration = totalDuration / 1000.0; // Convert from ms to s
        result.mTotalSize = totalSize;
        result.mMaxSpeed = maxSpeed;
        result.mMinSpeed = Double.compare(minSpeed, Double.MAX_VALUE) == 0 ? 0.0 : minSpeed; // Convert from MB/ms to MB/s
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
