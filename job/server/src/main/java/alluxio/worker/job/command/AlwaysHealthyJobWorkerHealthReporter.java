package alluxio.worker.job.command;

/**
 * {@link JobWorkerHealthReporter} that always reports that the worker is healthy.
 */
public class AlwaysHealthyJobWorkerHealthReporter extends JobWorkerHealthReporter {

  @Override
  public boolean isHealthy() {
    return true;
  }
}
