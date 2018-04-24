```java
FileSystem fs = FileSystem.Factory.get();
AlluxioURI uri = new AlluxioURI("/myFile");
SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(false);
fs.setAttribute(uri, pinOpt);
```
