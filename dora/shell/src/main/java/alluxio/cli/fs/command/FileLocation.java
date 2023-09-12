package alluxio.cli.fs.command;

import java.util.Objects;
import java.util.Set;

public class FileLocation {
  private final String fileName;

  private final String preferredWorker;

  private final boolean dataOnPreferredWorker;

  private final Set<String> workersThatHaveData;

  public FileLocation(String fileName, String preferredWorker, boolean dataOnPreferredWorker,
                      Set<String> workers) {
    this.fileName = fileName;
    this.preferredWorker = preferredWorker;
    this.dataOnPreferredWorker = dataOnPreferredWorker;
    this.workersThatHaveData = workers;
  }

  public String getFileName() {
    return fileName;
  }

  public String getPreferredWorker() {
    return preferredWorker;
  }

  public boolean isDataOnPreferredWorker() {
    return dataOnPreferredWorker;
  }

  public Set<String> getWorkersThatHaveData() {
    return workersThatHaveData;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FileLocation that = (FileLocation) o;
    return dataOnPreferredWorker == that.dataOnPreferredWorker &&
        Objects.equals(fileName, that.fileName) &&
        Objects.equals(preferredWorker, that.preferredWorker) &&
        Objects.equals(workersThatHaveData, that.workersThatHaveData);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileName, preferredWorker, dataOnPreferredWorker, workersThatHaveData);
  }
}