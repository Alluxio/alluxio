```java
TachyonFile file = TachyonFileSystem.open("/myFile");
SetStateOptions pinOpt = new SetStateOptions.Builder(ClientContext.getConf()).setPinned(false);
TachyonFileSystem.setState(file, pinOpt);
```
