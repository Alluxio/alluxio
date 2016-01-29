```scala
> val s = sc.textFile("tachyon-ft://stanbyHost:19998/foo")
> val double = s.map(line => line + line)
> double.saveAsTextFile("tachyon-ft://activeHost:19998/bar")
```
