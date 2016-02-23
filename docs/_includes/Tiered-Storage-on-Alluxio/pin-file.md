```java
AlluxioFile file = AlluxioFileSystem.open("/myFile");
SetStateOptions pinOpt = new SetStateOptions.Builder(ClientContext.getConf()).setPinned(true);
AlluxioFileSystem.setState(file, pinOpt);
```
