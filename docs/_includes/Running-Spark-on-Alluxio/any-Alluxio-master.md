```scala
> val s = sc.textFile("alluxio-ft://stanbyHost:19998/LICENSE")
> val double = s.map(line => line + line)
> double.saveAsTextFile("alluxio-ft://activeHost:19998/LICENSE2")
```
