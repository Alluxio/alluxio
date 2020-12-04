
1. metadata同步时间太长：

```
alluxio fs -Dalluxio.user.file.metadata.sync.interval=0 count /
```

能否支持并行fetch元数据
