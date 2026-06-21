package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class RankingProcessFunction
    extends ProcessAllWindowFunction<TradeAggregate, String, TimeWindow>
{

    private final int topN;

    public RankingProcessFunction(int topN) {
        this.topN = topN;
    }

    @Override
    public void process(
        Context context,
        Iterable<TradeAggregate> elements,
        Collector<String> out
    ) throws Exception {
        List<TradeAggregate> aggregates = new ArrayList<>();
        for (TradeAggregate agg : elements) {
            aggregates.add(agg);
        }
        RankingResult result = RankingCalculator.rank(
            aggregates,
            context.window().getStart(),
            context.window().getEnd(),
            topN
        );
        if (result.entries != null && !result.entries.isEmpty()) {
            out.collect(result.toString());
        }
    }
}
