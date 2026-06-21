package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class RankingCalculator {

    private static final double METRIC_EPSILON = 1e-9;

    public static RankingResult rank(
        List<TradeAggregate> aggregates,
        long windowStart,
        long windowEnd,
        int topN
    ) {
        if (aggregates == null || aggregates.isEmpty()) {
            return new RankingResult(windowStart, windowEnd, new ArrayList<>());
        }

        List<RankingEntry> entries = new ArrayList<>();
        for (TradeAggregate agg : aggregates) {
            double metric = agg.sumTradeVolume * agg.sumTradeAmount;
            entries.add(
                new RankingEntry(
                    0,
                    agg.code,
                    agg.sumTradeVolume,
                    agg.sumTradeAmount,
                    metric,
                    agg.tradeCount,
                    windowEnd
                )
            );
        }

        entries.sort(
            Comparator.comparingDouble(RankingEntry::getMetric)
                .reversed()
                .thenComparing(RankingEntry::getCode)
        );

        int rank = 0;
        double lastMetric = Double.NEGATIVE_INFINITY;
        for (int i = 0; i < entries.size(); i++) {
            RankingEntry entry = entries.get(i);
            if (
                i == 0 || Math.abs(entry.metric - lastMetric) > METRIC_EPSILON
            ) {
                rank++;
                lastMetric = entry.metric;
            }
            entry.rank = rank;
        }

        List<RankingEntry> topEntries = new ArrayList<>();
        for (RankingEntry entry : entries) {
            if (entry.rank <= topN) {
                topEntries.add(entry);
            }
        }

        return new RankingResult(windowStart, windowEnd, topEntries);
    }
}
