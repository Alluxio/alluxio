package alluxio.client.file;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Defines and Manages well-known Alluxio events.
 *
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 * !!! - Keep increasing the event-id for new events. !!!
 * !!! - Don't reassign event ids.                    !!!
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 */
public enum AlluxioEvent {
  // Alluxio master events.
  Master_Process_Starting(1, EventType.INFO, "Master process starting."),
  Master_Process_Stopping(2, EventType.INFO, "Master process stopping."),
  // Alluxio worker events.
  Worker_Process_Starting(3, EventType.INFO, "Worker process started."),
  Worker_Process_Stopping(4, EventType.INFO, "Worker process stopping.");

  // logger.
  private static final Logger LOG = LoggerFactory.getLogger(AlluxioEvent.class);

  /** The unique event id. */
  private final int mId;
  /** The event type. */
  private final EventType mEventType;
  /** The message string format. */
  private final String mMsgFormat;

  /**
   * Creates new Alluxio event type.
   *
   * @param eventId the unique event id
   * @param eventType the event type
   * @param msgFormat the event message (format string)
   */
  AlluxioEvent(int eventId, EventType eventType, String msgFormat) {
    mId = eventId;
    mEventType = eventType;
    mMsgFormat = msgFormat;
  }

  /**
   * Fires the event.
   *
   * @param args arguments to event message string
   */
  public void fire(Object... args) {
    String msgString = String.format(mMsgFormat, args);
    String eventString = String.format("Id: %d, Name: %s, Message:%s", mId, this.name(), msgString);
    switch (mEventType) {
      case INFO:
        LOG.info(eventString);
        break;
      case ERROR:
        LOG.error(eventString);
        break;
      case WARNING:
        LOG.warn(eventString);
        break;
      case DIAGNOSTIC:
        LOG.debug(eventString);
        break;
      default:
        throw new IllegalArgumentException(
            String.format("Unrecognized event type: %s", mEventType.name()));
    }
  }

  /**
   * Defines event levels.
   */
  enum EventType {
    INFO, WARNING, ERROR, DIAGNOSTIC
  }
}
