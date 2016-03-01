```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI uri = new AlluxioURI("/myFile");
SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(true);
fs.setAttribute(uri, pinOpt);
```
