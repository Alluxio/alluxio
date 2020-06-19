package alluxio;

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
  MasterProcessStarting(1000, EventType.INFO, "Master process starting."),
  JournalSystemStarted(1001, EventType.INFO, "Journal system started. %s"),
  MasterIsPrimary(1002, EventType.INFO, "Master process is now the primary."),
  MasterIsSecondary(1003, EventType.INFO, "Master process is now a secondary."),
  MasterIsTransitioning(1004, EventType.INFO, "Master process is transitioning to become: %s"),
  MasterProcessStopping(1005, EventType.INFO, "Master process stopping."),
  WorkerRegistered(1006, EventType.INFO, "Worker registered: %s"),
  WorkerLost(1007, EventType.INFO, "Worker lost: %s");

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
