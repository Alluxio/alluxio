package alluxio.cli.fsadmin.report;

import alluxio.client.job.JobMasterClient;
import alluxio.job.wire.JobInfo;
import alluxio.job.wire.JobServiceSummary;
import alluxio.job.wire.Status;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;

public class JobServiceMetricsCommandTest {

    private JobMasterClient mJobMasterClient;
    private ByteArrayOutputStream mOutputStream;
    private PrintStream mPrintStream;

    @Before
    public void before() throws IOException {
        mJobMasterClient = Mockito.mock(JobMasterClient.class);

        mOutputStream = new ByteArrayOutputStream();
        mPrintStream = new PrintStream(mOutputStream, true, "utf-8");
    }

    @After
    public void after() {
        mPrintStream.close();
    }

    @Test
    public void testBasic() throws IOException {

        Collection<JobInfo> jobInfos = new ArrayList<>();

        jobInfos.add(createJobInfoWithStatus(Status.RUNNING));
        jobInfos.add(createJobInfoWithStatus(Status.CANCELED));

        Mockito.when(mJobMasterClient.getJobServiceSummary()).thenReturn(new JobServiceSummary(jobInfos));

        new JobServiceMetricsCommand(mJobMasterClient, mPrintStream).run();

        String output = new String(mOutputStream.toByteArray(), StandardCharsets.UTF_8);

        Assert.assertEquals("", output);
    }

    private JobInfo createJobInfoWithStatus(Status status) {
        JobInfo jobInfo = new JobInfo();
        jobInfo.setStatus(status);
        return jobInfo;
    }

}
