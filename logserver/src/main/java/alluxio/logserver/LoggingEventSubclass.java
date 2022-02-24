package alluxio.logserver;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.apache.log4j.spi.LoggingEvent;

public class LoggingEventSubclass extends LoggingEvent {
    public LoggingEventSubclass(String fqnOfCategoryClass, Category logger, Priority level, Object message, Throwable throwable) {
        super(fqnOfCategoryClass, logger, level, message, throwable);
    }
}
