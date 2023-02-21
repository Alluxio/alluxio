package alluxio.job;

/**
 * Factory for creating job instances.
 */
public interface JobFactory {
  /**
   * @param request the job request
   * @return the job
   */
  Job create(JobRequest request);
}
