---
layout: global
title: Documentation Conventions
nickname: Documentation Conventions
group: Contributor Resources
priority: 2
---

* Table of Contents
{:toc}

This documentation provides a writing style guide that portrays professionalism and efficiency in delivering technical content
in Alluxio documentation.

## The C's

The C's, in order of importance:
1. Be correct
1. Be concise
1. Be consistent
1. Be ceremonial or formal (because ceremonial was the best synonym to **formal** that started with a C)

### Correctness = Don’t be wrong

No documentation is better than incorrect documentation.

* Information conveyed is accurate
* Use a spell checker to fix typos
* Capitalize acronyms
    * Ex. AWS, TTL, UFS, API, URL, SSH, I/O
* Capitalize proper nouns
    * Ex. Alluxio, Hadoop, Java

### Conciseness = Don’t use more words than necessary

No one wants to read more words than necessary.

* Use the [imperative mood](https://en.wikipedia.org/wiki/Imperative_mood), the same tone used when issuing a command
    * "Run the command to start the process"
    * **Not** "Next, you can run the command to start the process"
    * "Include a SocketAppender in the configuration..."
    * **Not** "A SocketAppender can be included in the configuration..."
* Use the [active voice](https://en.wikipedia.org/wiki/Active_voice)
    * "The process fails when misconfigured"
    * **Not** "The process will fail when misconfigured"
    * **Not** "It is known that starting the process will fail when misconfigured"
* Don’t use unnecessary punctuation
    * Avoid using parentheses to de-emphasize a section
        * Incorrect example: "Alluxio serves as a new data access layer in the ecosystem,
        residing between any persistent storage systems (such as Amazon S3, Microsoft Azure Object Store, Apache HDFS, or OpenStack Swift)
        and computation frameworks (such as Apache Spark, Presto, or Hadoop MapReduce)."
* Reduce the use of dependent clauses that add no content
    * Remove usages of the following:
        * For example, ...
        * However, ...
        * First, ...

### Consistency = Don’t use different forms of the same word or concept

There are many technical terms used throughout; it can potentially cause confusion when the same idea is expressed in multiple ways.

* See terminology table below
    * When in doubt, search to see how similar documentation expresses the same term
* Code-like text should be annotated with backticks
    * File paths
    * Property keys and values
    * Bash commands or flags
* Code blocks should be annotated with the associated file or usage type, e.g.:
    * ```` ```java```` for Java source code
    * ```` ```properties```` for a Java property file
    * ```` ```console```` for an interactive session in shell
    * ```` ```bash```` for a shell script
* Alluxio prefixed terms, such as namespace, cache, or storage, should be preceded by "the"
to differentiate from the commonly used term, but remain in lowercase if not a proper noun
    * Ex. The data will be copied into **the Alluxio storage**.
    * Ex. When a new file is added to **the Alluxio namespace**, ...
    * Ex. **The Alluxio master** never reads or writes data directly ...

### Formality = Don’t sound like a casual conversation

Documentation is not a conversation.
Don’t follow the same style as you would use when chatting with someone.

* Use the [serial comma](https://en.wikipedia.org/wiki/Serial_comma), also known as the Oxford comma, when listing items
    * Example: "Alluxio integrates with storage systems such as Amazon S3, Apache HDFS, and Microsoft Azure Object Store."
    Note the last comma after "HDFS".
* Avoid using contractions; remove the apostrophe and expand
    * Don’t -> Do not
* One space separates the ending period of a sentence and starting character of the next sentence;
this has been the norm [as of the 1950s](https://en.wikipedia.org/wiki/Sentence_spacing).
* Avoid using abbreviations
    * Doc -> Documentation

## Terminology table

| Correct, preferred term | Incorrect or less preferred term(s) |
|-------------------------|-----------------------------------------------------|
| File system | Filesystem |
| Leading master | Leader, lead master, primary master |
| Backup master | Secondary master, following master, follower master |
| Containerized | Dockerized |
| Superuser | Super-user, super user |
| I/O | i/o, IO |
| High availability mode | Fault tolerance mode (Use of "fault tolerance" is fine, but not when interchangeable with “high availability”) |
| Hostname | Host name |

## Line breaks

Each sentence starts in a new line for ease of reviewing diffs.
We do not have an official maximum characters per line for documentation files,
but feel free to split sentences into separate lines to avoid needing to scroll horizontally to read.

## Resources

* [General guidelines for technical writing style](https://en.wikiversity.org/wiki/Technical_writing_style)
* [Examples of moods](https://en.oxforddictionaries.com/grammar/moods)
* [Active vs passive voice examples](https://writing.wisc.edu/Handbook/CCS_activevoice.html)
