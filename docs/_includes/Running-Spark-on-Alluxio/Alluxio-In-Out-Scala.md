```scala
> val s = sc.textFile("alluxio://localhost:19998/foo")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/bar")
```
