```java
TachyonFile file = TachyonFileSystem.open("/myFile");
SetStateOptions pinOpt = new SetStateOptions.Builder(ClientContext.getConf()).setPinned(true);
TachyonFileSystem.setState(file, pinOpt);
```
