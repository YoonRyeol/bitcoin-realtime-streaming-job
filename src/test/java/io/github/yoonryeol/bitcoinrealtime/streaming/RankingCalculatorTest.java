package io.github.yoonryeol.bitcoinrealtime.streaming;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

class RankingCalculatorTest {

    @Test
    void testBasicRanking() {
        // Given
        List<TradeAggregate> aggregates = Arrays.asList(
            createAggregate("KRW-BTC", 100.0, 5000000.0, 10),
            createAggregate("KRW-ETH", 80.0, 4000000.0, 8),
            createAggregate("KRW-XRP", 60.0, 3000000.0, 6)
        );

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then
        assertThat(result).isNotNull();
        assertThat(result.windowStart).isEqualTo(1000L);
        assertThat(result.windowEnd).isEqualTo(2000L);
        assertThat(result.entries).hasSize(3);
        assertThat(result.entries.get(0).code).isEqualTo("KRW-BTC"); // highest metric
        assertThat(result.entries.get(0).rank).isEqualTo(1);
        assertThat(result.entries.get(1).rank).isEqualTo(2);
        assertThat(result.entries.get(2).rank).isEqualTo(3);
    }

    @Test
    void testDenseRankWithTies() {
        // Given: two with same metric (exactly 100000.0)
        List<TradeAggregate> aggregates = Arrays.asList(
            createAggregate("KRW-BTC", 100.0, 1000.0, 10), // metric = 100000.0
            createAggregate("KRW-ETH", 100.0, 1000.0, 8), // metric = 100000.0 (tie)
            createAggregate("KRW-XRP", 90.0, 900.0, 6) // metric = 81000.0
        );

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then: dense rank - same metric gets same rank
        assertThat(result.entries).hasSize(3);
        // BID is first due to tie-break (code asc), then ETH (same metric), then XRP (lower metric)
        assertThat(result.entries.get(0).rank).isEqualTo(1);
        assertThat(result.entries.get(0).code).isEqualTo("KRW-BTC");
        assertThat(result.entries.get(1).rank).isEqualTo(1); // same rank for tie (BID, ETH both have metric 100000)
        assertThat(result.entries.get(2).rank).isEqualTo(2); // XRP has lower metric (81000), so rank=2
    }

    @Test
    void testTop20Limit() {
        // Given: 25 entries
        List<TradeAggregate> aggregates = new java.util.ArrayList<>();
        for (int i = 0; i < 25; i++) {
            String code = String.format("KRW-XXX%02d", i);
            aggregates.add(createAggregate(code, 100.0 - i, 1000.0, 10));
        }

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then: only top 20 (rank <= 20)
        assertThat(result.entries).hasSize(20);
        assertThat(result.entries.get(19).rank).isEqualTo(20);
    }

    @Test
    void testEmptyInput() {
        // Given
        List<TradeAggregate> aggregates = null;

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then
        assertThat(result).isNotNull();
        assertThat(result.entries).isEmpty();
    }

    @Test
    void testNegativeMetric() {
        // Given: negative sumTradeAmount (more sell than buy)
        List<TradeAggregate> aggregates = Arrays.asList(
            createAggregate("KRW-BTC", 100.0, -500000.0, 10), // metric = -50000000
            createAggregate("KRW-ETH", 80.0, 400000.0, 8) // metric = 32000000
        );

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then: negative metric handled correctly
        assertThat(result.entries).hasSize(2);
        assertThat(result.entries.get(0).code).isEqualTo("KRW-ETH"); // positive metric first
        assertThat(result.entries.get(1).code).isEqualTo("KRW-BTC"); // negative metric second
    }

    @Test
    void testLessThan20Entries() {
        // Given: only 5 entries
        List<TradeAggregate> aggregates = Arrays.asList(
            createAggregate("KRW-BTC", 100.0, 1000.0, 10),
            createAggregate("KRW-ETH", 80.0, 800.0, 8),
            createAggregate("KRW-XRP", 60.0, 600.0, 6),
            createAggregate("KRW-SOL", 40.0, 400.0, 4),
            createAggregate("KRW-ADA", 20.0, 200.0, 2)
        );

        // When
        RankingResult result = RankingCalculator.rank(
            aggregates,
            1000L,
            2000L,
            20
        );

        // Then: all 5 entries (less than 20)
        assertThat(result.entries).hasSize(5);
        assertThat(result.entries.get(4).rank).isEqualTo(5);
    }

    private TradeAggregate createAggregate(
        String code,
        double volume,
        double amount,
        long count
    ) {
        TradeAggregate agg = new TradeAggregate();
        agg.setCode(code);
        agg.setSumTradeVolume(volume);
        agg.setSumTradeAmount(amount);
        agg.setTradeCount(count);
        return agg;
    }
}
