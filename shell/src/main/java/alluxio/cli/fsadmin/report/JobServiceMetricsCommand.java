package alluxio.cli.fsadmin.report;

import alluxio.client.job.JobMasterClient;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.Status;
import alluxio.job.wire.StatusSummary;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;

public class JobServiceMetricsCommand {

    private final JobMasterClient mJobMasterClient;
    private final PrintStream mPrintStream;

    /**
     * Creates a new instance of {@link JobServiceMetricsCommand}.
     *
     * @param JobMasterClient client to connect to job master client
     * @param printStream stream to print job services metrics information to
     */
    public JobServiceMetricsCommand(JobMasterClient JobMasterClient, PrintStream printStream) {
        mJobMasterClient = JobMasterClient;
        mPrintStream = printStream;
    }

    /**
     * Runs a job services report metrics command.
     *
     * @return 0 on success, 1 otherwise
     */
    public int run() throws IOException {

        JobServiceSummary jobServiceSummary = mJobMasterClient.getJobServiceSummary();

        Collection<StatusSummary> jobStatusSummaries = jobServiceSummary.getSummaryPerStatus();

        for (StatusSummary statusSummary : jobStatusSummaries) {
            mPrintStream.print(String.format("Status: %-10s", statusSummary.getStatus()));
            mPrintStream.print(String.format("Count: %s", statusSummary.getCount()));
        }
    }

}
