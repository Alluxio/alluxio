package alluxio.conf.reconf;

/**
 * Reconfigurable listener.
 */
public interface ReconfigurableListener {
  /**
   * When the property changed, this function will be invoked.
   */
  void propertyChange();
}
