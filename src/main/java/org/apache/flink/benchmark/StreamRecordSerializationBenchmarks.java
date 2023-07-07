package org.apache.flink.benchmark;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

public class StreamRecordSerializationBenchmarks extends BenchmarkBase {
    final int NUM_RECORDS = 1_000_000;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + StreamRecordSerializationBenchmarks.class.getCanonicalName() + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    @OperationsPerInvocation(value = NUM_RECORDS)
    public void baseline(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.getConfig().setAutoWatermarkInterval(0);
        env.getConfig().setOperatorTimestamp(true);
        env.getConfig().enableObjectReuse();

        DataStream<Boolean> stream = env.fromSequence(0, NUM_RECORDS).map(x -> true);
        for (int i = 0; i < 10; i++) {
            stream = stream.map(x -> x).disableChaining();
        }
        stream.addSink(new DiscardingSink<>());

        env.execute();
    }

    @Benchmark
    @OperationsPerInvocation(value = NUM_RECORDS)
    public void withTimestampOptimizeEnabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.getConfig().setAutoWatermarkInterval(0);
        env.getConfig().setOperatorTimestamp(false);
        env.getConfig().enableObjectReuse();

        DataStream<Boolean> stream = env.fromSequence(0, NUM_RECORDS).map(x -> true);
        for (int i = 0; i < 10; i++) {
            stream = stream.map(x -> x).disableChaining();
        }
        stream.addSink(new DiscardingSink<>());

        env.execute();
    }
}
