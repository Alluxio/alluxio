```java
AlluxioURI uri = new AlluxioURI("/myFile");
SetAttributeOptions pinOpt = SetAttributeOptions.defaults().setPinned(false);
FileSystem.Factory.get().setAttribute(uri, pinOpt);
```
