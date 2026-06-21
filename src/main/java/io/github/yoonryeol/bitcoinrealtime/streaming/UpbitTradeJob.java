package io.github.yoonryeol.bitcoinrealtime.streaming;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UpbitTradeJob {

    public static void main(String[] args) throws Exception {
        JobConfig config = new JobConfig();

        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(config.parallelism);
        env.setStateBackend(
            new org.apache.flink.runtime.state.hashmap.HashMapStateBackend()
        );
        env.enableCheckpointing(
            config.checkpointIntervalMs,
            config.checkpointMode
        );

        KafkaSource<UpbitTrade> source = KafkaSource.<UpbitTrade>builder()
            .setBootstrapServers(config.kafkaBootstrapServers)
            .setTopics(config.kafkaTopic)
            .setGroupId(config.kafkaGroupId)
            .setStartingOffsets(config.kafkaStartOffset)
            .setDeserializer(new UpbitTradeDeserializationSchema())
            .build();

        WatermarkStrategy<UpbitTrade> watermarkStrategy = WatermarkStrategy.<
            UpbitTrade
        >forBoundedOutOfOrderness(config.watermarkDelay).withTimestampAssigner(
            (event, timestamp) -> event.tradeTimestamp
        );

        DataStream<UpbitTrade> inputStream = env.fromSource(
            source,
            watermarkStrategy,
            "KafkaSource",
            TypeInformation.of(UpbitTrade.class)
        );

        // Dedup
        DataStream<UpbitTrade> dedupedStream = inputStream
            .keyBy(t -> t.code + "|" + t.sequentialId)
            .process(new TradeDedupFunction(config.dedupTtl));

        // Windowed aggregation by code
        DataStream<TradeAggregate> aggregated = dedupedStream
            .keyBy(t -> t.code)
            .window(
                SlidingEventTimeWindows.of(
                    Time.minutes(config.windowSize.toMinutes()),
                    Time.minutes(config.windowSlide.toMinutes())
                )
            )
            .aggregate(new TradeAggregator());

        // Global ranking with windowAll
        DataStream<String> result = aggregated
            .windowAll(
                SlidingEventTimeWindows.of(
                    Time.minutes(config.windowSize.toMinutes()),
                    Time.minutes(config.windowSlide.toMinutes())
                )
            )
            .process(new RankingProcessFunction(config.topN));

        result.print();
        env.execute("Bitcoin Realtime Streaming Job");
    }
}
