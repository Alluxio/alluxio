package alluxio.conf.reconf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;

/**
 * ReconfigurableModel manager listeners and fire the event.
 */
public class ReconfigurableModel {
  public static final Log LOG = LogFactory.getLog(ReconfigurableModel.class);
  private static final ReconfigurableModel INSTANCE = new ReconfigurableModel();
  private List<ReconfigurableListener> mListenerList = new LinkedList<>();

  /**
   * @return the instance
   */
  public static ReconfigurableModel getInstance() {
    return INSTANCE;
  }

  /**
   * Add a listener.
   *
   * @param listener the given property listener
   * @return return current instance
   */
  public synchronized ReconfigurableModel addListener(ReconfigurableListener listener) {
    mListenerList.add(listener);
    return INSTANCE;
  }

  /**
   * remove the listener related to the given property.
   * @param listener the listener
   * @return the instance
   */
  public synchronized ReconfigurableModel removeListener(
      ReconfigurableListener listener) {
    mListenerList.remove(listener);
    return INSTANCE;
  }

  /**
   * When the property was reconfigured, this function will be invoked.
   * This property listeners will be notified.
   *
   * @return false if no listener related to the given property, otherwise, return false
   */
  public synchronized boolean propertyChange() {
    for (ReconfigurableListener listener : mListenerList) {
      listener.propertyChange();
    }
    return true;
  }
}
