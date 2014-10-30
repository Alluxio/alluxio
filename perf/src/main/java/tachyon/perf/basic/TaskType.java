package tachyon.perf.basic;

import java.io.File;
import java.util.Map;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.log4j.Logger;

import tachyon.perf.conf.PerfConf;
import tachyon.perf.util.SAXTaskType;

/**
 * Manage the different type of tasks.
 */
public class TaskType {
  private static final Logger LOG = Logger.getLogger("");

  private static TaskType sTaskType = null;

  public static synchronized TaskType get() {
    if (sTaskType == null) {
      try {
        sTaskType = new TaskType(PerfConf.get().TACHYON_PERF_HOME + "/conf/task-type.xml");
      } catch (Exception e) {
        LOG.error("Error when parse conf/task-type.xml", e);
        throw new RuntimeException("Failed to parse conf/task-type.xml");
      }
    }
    return sTaskType;
  }

  private Map<String, String> mTaskClasses;
  private Map<String, String> mTaskContextClasses;
  private Map<String, String> mTaskThreadClasses;
  private Map<String, String> mTotalReportClasses;

  private TaskType(String xmlFileName) throws Exception {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    SAXParser saxParser = spf.newSAXParser();
    File xmlFile = new File(xmlFileName);
    SAXTaskType saxTaskType = new SAXTaskType();
    saxParser.parse(xmlFile, saxTaskType);
    mTaskClasses = saxTaskType.getTaskClasses();
    mTaskContextClasses = saxTaskType.getTaskContextClasses();
    mTaskThreadClasses = saxTaskType.getTaskThreadClasses();
    mTotalReportClasses = saxTaskType.getTotalReportClasses();
  }

  /**
   * Get the task class of specified task. The class should be a subclass of PerfTask.
   * 
   * @param type the type of the task
   * @return the task class
   * @throws Exception
   */
  public PerfTask getTaskClass(String type) throws Exception {
    String taskClassName = mTaskClasses.get(type);
    return (PerfTask) Class.forName(taskClassName).newInstance();
  }

  /**
   * Get the task context class of specified task. The class should be a subclass of TaskContext.
   * 
   * @param type the type of the task
   * @return the task context class
   * @throws Exception
   */
  public PerfTaskContext getTaskContextClass(String type) throws Exception {
    String taskContextClassName = mTaskContextClasses.get(type);
    return (PerfTaskContext) Class.forName(taskContextClassName).newInstance();
  }

  /**
   * Get the task thread class of specified task. The class should be a subclass of PerfThread.
   * 
   * @param type the type of the task
   * @return the task thread class
   * @throws Exception
   */
  public PerfThread getTaskThreadClass(String type) throws Exception {
    String taskThreadClassName = mTaskThreadClasses.get(type);
    return (PerfThread) Class.forName(taskThreadClassName).newInstance();
  }

  /**
   * Get the task total report of specified task. The class should be a subclass of PerfTotalReport.
   * 
   * @param type the type of the task
   * @return the task total report class
   * @throws Exception
   */
  public PerfTotalReport getTotalReportClass(String type) throws Exception {
    String totalReportClassName = mTotalReportClasses.get(type);
    return (PerfTotalReport) Class.forName(totalReportClassName).newInstance();
  }
}
