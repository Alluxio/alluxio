```java
FileSystem fs = FileSystem.Factory.get();
TachyonURI path = new TachyonURI("/myFile");
// Create a file and get its output stream
FileOutStream out = fs.createFile(path);
// Write data
out.write(...);
// Close and complete file
out.close();
```
