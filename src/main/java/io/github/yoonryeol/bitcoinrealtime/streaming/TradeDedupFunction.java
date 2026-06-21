package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.time.Duration;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class TradeDedupFunction
    extends KeyedProcessFunction<String, UpbitTrade, UpbitTrade>
{

    private final Duration ttl;
    private transient ValueState<Boolean> seenState;

    public TradeDedupFunction(Duration ttl) {
        this.ttl = ttl;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
            "seen",
            Boolean.class
        );
        org.apache.flink.api.common.state.StateTtlConfig ttlConfig =
            org.apache.flink.api.common.state.StateTtlConfig.newBuilder(
                Time.minutes(ttl.toMinutes())
            )
                .setUpdateType(
                    org.apache.flink.api.common.state.StateTtlConfig.UpdateType.OnCreateAndWrite
                )
                .build();
        descriptor.enableTimeToLive(ttlConfig);
        seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(
        UpbitTrade value,
        Context context,
        Collector<UpbitTrade> out
    ) throws Exception {
        Boolean seen = seenState.value();
        if (seen == null) {
            seenState.update(true);
            out.collect(value);
        }
    }
}
