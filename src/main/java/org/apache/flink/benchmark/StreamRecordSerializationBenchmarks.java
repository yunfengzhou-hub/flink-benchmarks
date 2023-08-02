package org.apache.flink.benchmark;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.util.Collector;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

public class StreamRecordSerializationBenchmarks extends BenchmarkBase {
    static final int NUM_RECORDS = 1_000_000;

    static final String[] WORDS_SAMPLE =
            new String[] {
                    "To be, or not to be,--that is the question:--",
                    "Whether 'tis nobler in the mind to suffer",
                    "The slings and arrows of outrageous fortune",
                    "Or to take arms against a sea of troubles,",
                    "And by opposing end them?--To die,--to sleep,--",
                    "No more; and by a sleep to say we end",
                    "The heartache, and the thousand natural shocks",
                    "That flesh is heir to,--'tis a consummation",
                    "Devoutly to be wish'd. To die,--to sleep;--",
                    "To sleep! perchance to dream:--ay, there's the rub;",
                    "For in that sleep of death what dreams may come,",
                    "When we have shuffled off this mortal coil,",
                    "Must give us pause: there's the respect",
                    "That makes calamity of so long life;",
                    "For who would bear the whips and scorns of time,",
                    "The oppressor's wrong, the proud man's contumely,",
                    "The pangs of despis'd love, the law's delay,",
                    "The insolence of office, and the spurns",
                    "That patient merit of the unworthy takes,",
                    "When he himself might his quietus make",
                    "With a bare bodkin? who would these fardels bear,",
                    "To grunt and sweat under a weary life,",
                    "But that the dread of something after death,--",
                    "The undiscover'd country, from whose bourn",
                    "No traveller returns,--puzzles the will,",
                    "And makes us rather bear those ills we have",
                    "Than fly to others that we know not of?",
                    "Thus conscience does make cowards of us all;",
                    "And thus the native hue of resolution",
                    "Is sicklied o'er with the pale cast of thought;",
                    "And enterprises of great pith and moment,",
                    "With this regard, their currents turn awry,",
                    "And lose the name of action.--Soft you now!",
                    "The fair Ophelia!--Nymph, in thy orisons",
                    "Be all my sins remember'd."
            };

    static final String[] WORDS = new String[NUM_RECORDS];

    static {{
        int WORDS_LEN = WORDS_SAMPLE.length;

        for (int i = 0; i < NUM_RECORDS; i++) {
            WORDS[i] = WORDS_SAMPLE[i % WORDS_LEN];
        }
    }
    }

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

    @Benchmark
    @OperationsPerInvocation(value = NUM_RECORDS)
    public void wordCount(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.getConfig().setAutoWatermarkInterval(0);

        wordCount(env);
    }

    @Benchmark
    @OperationsPerInvocation(value = NUM_RECORDS)
    public void wordCountWithTimestampOptimizeEnabled(FlinkEnvironmentContext context) throws Exception {
        StreamExecutionEnvironment env = context.env;
        env.getConfig().setAutoWatermarkInterval(0);
        env.getConfig().setOperatorTimestamp(false);

        wordCount(env);
    }

    private static void wordCount(StreamExecutionEnvironment env) throws Exception {
        DataStream<String> text= env.fromElements(WORDS).name("in-memory-input");

        DataStream<Tuple2<String, Integer>> counts =
                text.flatMap(new Tokenizer())
                        .disableChaining()
                        .name("tokenizer")
                        .keyBy(value -> value.f0)
                        .sum(1)
                        .disableChaining()
                        .name("counter");

        counts.addSink(new DiscardingSink<>());

        env.execute("WordCount");
    }

    public static final class Tokenizer
            implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }
}
