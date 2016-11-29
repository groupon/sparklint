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

##### Live mode (run inside spark driver node)

SparklintListener is an implementation of [SparkFirehoseListener](https://spark.apache.org/docs/1.5.2/api/java/org/apache/spark/SparkFirehoseListener.html)
that listen to spark event log while the application is running.

Simply add these lines to your spark-submit script:

```
--conf spark.extraListeners=com.groupon.sparklint.SparklintListener --packages com.groupon.sparklint:sparklint:1.0.2
```

Or add com.groupon.sparklint:sparklint to your maven/sbt dependency, and add the following line:

```
--conf spark.extraListeners=com.groupon.sparklint.SparklintListener
```

Then find out your spark application's driver node address, open a browser and visit port 23763 of the driver node.

##### Server mode (run on local machine)

SparklintServer can run on your local machine. It will read spark event logs from the location specified.
You can feed Sparklint an event log file to playback activities.

- Checking on the repo
- Make sure you have [SBT](http://www.scala-sbt.org/) installed.
- Copy spark event log files to analyze into a directory then `sbt "run -d /path/to/log/dir -r"`
- Or analyze a single log file `sbt "run -f /path/to/logfile -r"`
- Then open browser and navigate to `http://localhost:23763`

The command line arguments supported are:

- `-f [FileName]`: Filename of an Spark event log source to use.
- `-d [DirectoryName]`: Directory of an Spark event log sources to use. Read in filename sort order.
- `-p [pollRate]`: The interval (in seconds) between polling for changes in directory and history event sources.
- `-r`: Set the flag in order to run each buffer through to their end state on startup.


For more detail about event logging, how to enable it, and how to gather log files, check http://spark.apache.org/docs/latest/configuration.html#spark-ui
