```java
FileSystem fs = FileSystem.Factory.get();
TachyonURI path = new TachyonURI("/myFile");
// Open the file for reading and obtains a lock preventing deletion
FileInStream in = fs.openFile(path);
// Read data
in.read(...);
// Close file relinquishing the lock
in.close();
```
