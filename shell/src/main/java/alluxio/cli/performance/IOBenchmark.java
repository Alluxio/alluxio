package alluxio.cli.performance;

import alluxio.cli.AbstractShell;
import alluxio.cli.Command;
import alluxio.client.job.JobMasterClient;
import alluxio.job.plan.io.IOConfig;

import java.util.Map;

public class IOBenchmark {
    public long write() {
        // Submit one IOConfig with WRITE mode
        // Poll the job status and time it
        return 0;
    }
    public long read() {
        // Submit one IOConfig with READ mode
        // Poll the job status and time it
        return 0;
    }
    public static void main(String[] args) {
//        for (int i=0; i<iteration; i++) {
//            long writeTime = write();
//            long readTime = read();
//        }
        // Calculate the stats and format output
    }
}
