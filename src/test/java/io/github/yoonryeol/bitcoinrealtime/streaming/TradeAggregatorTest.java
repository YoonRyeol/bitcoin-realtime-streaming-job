package io.github.yoonryeol.bitcoinrealtime.streaming;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class TradeAggregatorTest {

    @Test
    void testAddWithBid() {
        // Given
        TradeAggregator aggregator = new TradeAggregator();
        UpbitTrade trade = createTrade("KRW-BTC", 100.0, 1.5, "BID", 1000L);

        // When
        TradeAggregate accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(trade, accumulator);

        // Then
        assertThat(accumulator.sumTradeVolume).isEqualTo(1.5);
        assertThat(accumulator.sumTradeAmount).isEqualTo(150.0); // 100 * 1.5 (BID = +)
        assertThat(accumulator.tradeCount).isEqualTo(1);
    }

    @Test
    void testAddWithAsk() {
        // Given
        TradeAggregator aggregator = new TradeAggregator();
        UpbitTrade trade = createTrade("KRW-BTC", 100.0, 1.5, "ASK", 1000L);

        // When
        TradeAggregate accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(trade, accumulator);

        // Then
        assertThat(accumulator.sumTradeVolume).isEqualTo(1.5);
        assertThat(accumulator.sumTradeAmount).isEqualTo(-150.0); // 100 * 1.5 (ASK = -)
        assertThat(accumulator.tradeCount).isEqualTo(1);
    }

    @Test
    void testMerge() {
        // Given
        TradeAggregate a = new TradeAggregate("KRW-BTC");
        a.sumTradeVolume = 100.0;
        a.sumTradeAmount = 5000.0;
        a.tradeCount = 5;

        TradeAggregate b = new TradeAggregate("KRW-BTC");
        b.sumTradeVolume = 80.0;
        b.sumTradeAmount = 4000.0;
        b.tradeCount = 4;

        // When
        TradeAggregate merged = new TradeAggregator().merge(a, b);

        // Then
        assertThat(merged.sumTradeVolume).isEqualTo(180.0);
        assertThat(merged.sumTradeAmount).isEqualTo(9000.0);
        assertThat(merged.tradeCount).isEqualTo(9);
    }

    @Test
    void testMultipleAdds() {
        // Given
        TradeAggregator aggregator = new TradeAggregator();
        List<UpbitTrade> trades = new ArrayList<>();
        trades.add(createTrade("KRW-BTC", 100.0, 1.5, "BID", 1000L));
        trades.add(createTrade("KRW-BTC", 110.0, 2.0, "BID", 1001L));
        trades.add(createTrade("KRW-BTC", 90.0, 1.0, "ASK", 1002L));

        // When
        TradeAggregate accumulator = aggregator.createAccumulator();
        for (UpbitTrade trade : trades) {
            accumulator = aggregator.add(trade, accumulator);
        }

        // Then
        assertThat(accumulator.sumTradeVolume).isEqualTo(4.5);
        assertThat(accumulator.sumTradeAmount).isEqualTo(280.0); // 100*1.5 + 110*2.0 - 90*1.0
        assertThat(accumulator.tradeCount).isEqualTo(3);
    }

    @Test
    void testAddSetsCode() {
        // Given
        TradeAggregator aggregator = new TradeAggregator();
        UpbitTrade trade = createTrade("KRW-BTC", 100.0, 1.5, "BID", 1000L);

        // When
        TradeAggregate accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(trade, accumulator);

        // Then: code가 trade.code와 동일해야 함
        assertThat(accumulator.code).isEqualTo("KRW-BTC");
    }

    @Test
    void testGetResultPreservesCode() {
        // Given
        TradeAggregator aggregator = new TradeAggregator();
        UpbitTrade trade = createTrade("KRW-ETH", 200.0, 2.0, "ASK", 1000L);

        // When
        TradeAggregate accumulator = aggregator.createAccumulator();
        accumulator = aggregator.add(trade, accumulator);
        TradeAggregate result = aggregator.getResult(accumulator);

        // Then: getResult()가 반환한 객체의 code가 null이 아니어야 함
        assertThat(result.code).isNotNull().isEqualTo("KRW-ETH");
    }

    @Test
    void testMergePreservesCode() {
        // Given: 동일한 code를 가진 두 accumulator
        TradeAggregate a = new TradeAggregate("KRW-BTC");
        a.sumTradeVolume = 100.0;
        a.sumTradeAmount = 5000.0;
        a.tradeCount = 5;

        TradeAggregate b = new TradeAggregate("KRW-BTC");
        b.sumTradeVolume = 80.0;
        b.sumTradeAmount = 4000.0;
        b.tradeCount = 4;

        // When
        TradeAggregate merged = new TradeAggregator().merge(a, b);

        // Then: merge 결과의 code가 유지되어야 함
        assertThat(merged.code).isEqualTo("KRW-BTC");
    }

    private UpbitTrade createTrade(
        String code,
        double price,
        double volume,
        String askBid,
        long timestamp
    ) {
        UpbitTrade trade = new UpbitTrade();
        trade.code = code;
        trade.tradePrice = price;
        trade.tradeVolume = volume;
        trade.askBid = askBid;
        trade.tradeTimestamp = timestamp;
        return trade;
    }
}
