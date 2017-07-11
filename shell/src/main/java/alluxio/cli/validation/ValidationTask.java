package alluxio.cli.validation;

/**
 * Interface for a validation task run by validateEnv command.
 */
public interface ValidationTask {
  /**
   * Runs the validation task.
   * @return whether the validation succeeds
   */
  boolean validate();
}
