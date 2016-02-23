```java
AlluxioURI uri = new AlluxioURI("/myFile");
SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(true);
FileSystem.Factory.get().setAttribute(uri, pinOpt);
```
