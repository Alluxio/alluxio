Alluxio (formerly Tachyon)
=======

The master branch is in version 1.1.0-SNAPSHOT:

- [Alluxio Website](http://www.alluxio.org/) | [Alluxio Latest Release Document](http://www.alluxio.org/documentation/) | [Master Branch Document](http://alluxio.org/documentation/master/)
- [Contribute to Alluxio](http://alluxio.org/documentation/master/en/Contributing-to-Alluxio.html) and
[New Contributor Tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20NewContributor%20AND%20status%20%3D%20Open)
  - Please limit 2 tasks per new contributor. Afterwards, try some [beginner tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20Beginner%20AND%20status%20%3D%20Open) or [intermediate tasks](https://alluxio.atlassian.net/issues/?jql=project%20%3D%20ALLUXIO%20AND%20labels%20%3D%20Intermediate%20AND%20status%20%3D%20Open),
  or ask in the [Developer Mailing List](https://groups.google.com/forum/#!forum/alluxio-dev).
- [Releases](http://alluxio.org/releases/)
- [Downloads](http://alluxio.org/downloads/)
- [JIRA](https://alluxio.atlassian.net/browse/ALLUXIO)
- [User Mailing List](https://groups.google.com/forum/?fromgroups#!forum/alluxio-users)
- [Bay Area Meetup Group](http://www.meetup.com/Alluxio)

## Building applications with Alluxio

### Dependency Information

#### Apache Maven
```xml
<dependency>
  <groupId>org.alluxio</groupId>
  <artifactId>alluxio-core-client</artifactId>
  <version>1.0.1</version>
</dependency>
```

#### Gradle

```groovy
compile 'org.alluxio:alluxio-core-client:1.0.1'
```

#### Apache Ant
```xml
<dependency org="org.alluxio" name="alluxio" rev="1.0.1">
  <artifact name="alluxio-core-client" type="jar" />
</dependency>
```

#### SBT
```
libraryDependencies += "org.alluxio" % "alluxio-core-client" % "1.0.1"
```

## Contributing to Alluxio

Contributions via GitHub pull requests are gladly accepted from their original author. Along with
any pull requests, please state that the contribution is your original work and that you license the
work to the project under the project's open source license. Whether or not you state this
explicitly, by submitting any copyrighted material via pull request, email, or other means you agree
to license the material under the project's open source license and warrant that you have the legal
authority to do so.
