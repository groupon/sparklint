Sparklint
========

The missing Spark Performance Debugger that can be drag and dropped into your spark application!

### Mission
- Provide advance metrics and better visualization about your spark application's resource utilization
  - Vital application life time stats like idle time, average vcore usage, distribution of locality
  - View task allocation by executor
  - VCore usage graphs
  - (WIP) Find rdds that can benefit from persisting
  - (WIP) View task execution stats by locality
- Help you find out where the bottle neck are
  - (WIP) Automated report on application bottle neck
- (WIP) Opportunity to give the running application real-time hints on magic numbers like partitions size, job submission parallelism, whether to persist rdd or not

![ScreenShot](screen_shot.png)

### Usage

First figure out your current spark version. Please refer to `project/BuildUtils.scala` and look for `SUPPORTED_SPARK_VERSIONS`, these are the versions that we support out of box.

If your spark version has our precompiled sparklint jar (let's say 1.6.1 on scala 2.10), the jar name will be `sparklint-spark161_2.10`.
Please note, in the jar the `161` means `Spark1.6.1` and `2.10` means `scala 2.10`

If your spark version is not precompiled (i.e. 1.5.0), you can add an entry in `project/BuildUtils.getSparkMajorVersion`, then provide compatible code similar to spark-1.6 in `src/main/spark-1.5`

##### Live mode (run inside spark driver node)

SparklintListener is an implementation of [SparkFirehoseListener](https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/SparkFirehoseListener.html)
that listen to spark event log while the application is running. To enable it, you can try one of the following:

1. Upload packaged jar to your cluster, include jar in classpath directly
2. Use `--packages` command to inject dependency during job submission if we have a precompiled jar, like `--conf spark.extraListeners=com.groupon.sparklint.SparklintListener --packages com.groupon.sparklint:sparklint-spark161_2.10:1.0.4`
3. Add dependency directly in your pom, repackage your application, then during job submission, use `--conf spark.extraListeners=com.groupon.sparklint.SparklintListener`

Finally, find out your spark application's driver node address, open a browser and visit port 23763 (our default port) of the driver node.

> Add dependency directly for pom.xml
  ```
  <dependency>
      <groupId>com.groupon.sparklint</groupId>
      <artifactId>sparklint-spark161_2.10</artifactId>
      <version>1.0.4</version>
  </dependency>
  ```
  for build.sbt
  ```
  libraryDependencies += "com.groupon.sparklint" %% "sparklint-spark161" % "1.0.4"
  ```

##### Server mode (run on local machine)

SparklintServer can run on your local machine. It will read spark event logs from the location specified.
You can feed Sparklint an event log file to playback activities.

- Checking out the repo
- Make sure you have [SBT](http://www.scala-sbt.org/) installed.
- Copy spark event log files to analyze into a directory then `sbt "run -d /path/to/log/dir -r"`
- Or analyze a single log file `sbt "run -f /path/to/logfile -r"`
- Then open browser and navigate to `http://localhost:23763`
- Docker support is comming soon, track progress at #26
- Spark version doesn't matter in server mode

The command line arguments supported are:

- `-f [FileName]`: Filename of an Spark event log source to use.
- `-d [DirectoryName]`: Directory of an Spark event log sources to use. Read in filename sort order.
- `-p [pollRate]`: The interval (in seconds) between polling for changes in directory and history event sources.
- `-r`: Set the flag in order to run each buffer through to their end state on startup.


For more detail about event logging, how to enable it, and how to gather log files, check http://spark.apache.org/docs/latest/configuration.html#spark-ui

### Developer cheatsheet

* First enter sbt console `sbt`
* Test: `test`
* Cross Scala version test `+ test`
* Rerun failed tests: `testQuick`
* Change spark version: `set sparkVersion := "2.0.0"`
* Change scala version: `++ 2.11.8`
* Package: `package`
* Perform task (e.g, test) foreach spark version `foreachSparkVersion test`
* Publish to sonatype staging `foreachSparkVersion publishSigned`

