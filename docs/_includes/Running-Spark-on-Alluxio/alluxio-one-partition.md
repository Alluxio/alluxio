```scala
> val s = sc.textFile("alluxio://localhost:19998/LICENSE", 1)
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio://localhost:19998/LICENSE2")
```
