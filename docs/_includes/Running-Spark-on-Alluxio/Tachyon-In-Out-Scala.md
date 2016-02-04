```scala
> val s = sc.textFile("tachyon://localhost:19998/foo")
> val double = s.map(line => line + line)
> double.saveAsTextFile("tachyon://localhost:19998/bar")
```
