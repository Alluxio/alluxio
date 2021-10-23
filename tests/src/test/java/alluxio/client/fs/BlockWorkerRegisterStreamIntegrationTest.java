package alluxio.client.fs;

/**
 * Integration tests for the client-side logic for the register stream.
 */
public class BlockWorkerRegisterStreamIntegrationTest {

  /**
   * Tests below cover the most normal cases.
   */
  // TODO(jiacheng): generate requests for an empty worker

  // TODO(jiacheng): generate requests for multiple batches



  /**
   * Tests below cover various failure cases.
   */
  // TODO(jiacheng): master fails in workerRegisterStart, worker should see the error

  // TODO(jiacheng): master fails in workerRegisterStream, worker should see the error

  // TODO(jiacheng): master fails in workerRegisterComplete, worker should see the error

  // TODO(jiacheng): master hangs during the stream, worker should see the timeout

  // TODO(jiacheng): deadline exceeded?


  /**
   * Tests below cover the race conditions during concurrent executions.
   *
   * If a worker is new to the cluster, no clients should know this worker.
   * Therefore there is no concurrent client-incurred write operations on this worker.
   * The races happen typically when the worker re-registers with the master,
   * where some clients already know this worker and can direct invoke writes on the worker.
   *
   * Tests here verify the integrity of the worker-side metadata.
   * In other words, even a commit/delete happens on the worker during the register stream,
   * the change should be successful and the update should be recorded correctly.
   * The update should later be reported to the master.
   */
  // TODO(jiacheng): register streaming, a delete happened, check the following heartbeat

  // TODO(jiacheng): register streaming, a commit happened, check the following heartbeat
}
