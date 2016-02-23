```java
AlluxioFile file = AlluxioFileSystem.open("/myFile");
SetStateOptions pinOpt = new SetStateOptions.Builder(ClientContext.getConf()).setPinned(false);
AlluxioFileSystem.setState(file, pinOpt);
```
