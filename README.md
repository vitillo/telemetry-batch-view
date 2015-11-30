# telemetry-batch-view

This is a Scala "framework" to build derived datasets, aka batch views, of [Telemetry](https://wiki.mozilla.org/Telemetry) data.

Raw JSON [pings](https://ci.mozilla.org/job/mozilla-central-docs/Tree_Documentation/toolkit/components/telemetry/telemetry/pings.html) are stored on S3 within files containing [framed Heka records](https://hekad.readthedocs.org/en/latest/message/index.html#stream-framing). Reading the raw data in through e.g. Spark can be slow as for a given analysis only a few fields are typically used; not to mention the cost of parsing the JSON blobs. Furthermore, Heka files might contain only a handful of records under certain circumstances.

Defining a derived [Parquet](https://parquet.apache.org/) dataset, which uses a columnar layout optimized for analytics workloads, can drastically improve the performance of analysis jobs while reducing the space requirements. A derived dataset might, and should, also perform heavy duty operations common to all analysis that are going to read from that dataset.

The converted datasets are stored in the bucket specified in [*application.conf*](https://github.com/vitillo/aws-lambda-parquet/blob/master/src/main/resources/application.conf#L2).

### Adding a new derived dataset

See the [streams](https://github.com/vitillo/telemetry-parquet/blob/master/src/main/scala/streams) folder for the currently defined datasets.

### Local execution
Given a subtype of `DerivedStream` of type `MyStream`, a dataset for e.g. the 28th of October can be generated with:
```
sbt "run-main telemetry.DerivedStream --from-date 20151028 --to-date 20151028 MyStream"
```

### Distributed execution 
Build an uber-jar with:
```
sbt assembly
```
then, submit the job:
```
spark-submit --master yarn-client --class telemetry.DerivedStream target/scala-2.10/telemetry-parquet-converter.jar --from-date 20151028 --to-date 20151028 MyStream
```
