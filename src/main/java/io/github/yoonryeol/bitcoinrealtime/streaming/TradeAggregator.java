package io.github.yoonryeol.bitcoinrealtime.streaming;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.AggregateFunction;

public class TradeAggregator
    implements AggregateFunction<UpbitTrade, TradeAggregate, TradeAggregate>
{

    @Override
    public TradeAggregate createAccumulator() {
        return new TradeAggregate();
    }

    @Override
    public TradeAggregate add(UpbitTrade trade, TradeAggregate accumulator) {
        double sign = "BID".equals(trade.askBid) ? 1.0 : -1.0;
        accumulator.code = trade.code;
        accumulator.sumTradeVolume += trade.tradeVolume;
        accumulator.sumTradeAmount +=
            sign * trade.tradePrice * trade.tradeVolume;
        accumulator.tradeCount++;
        return accumulator;
    }

    @Override
    public TradeAggregate getResult(TradeAggregate accumulator) {
        return accumulator;
    }

    @Override
    public TradeAggregate merge(TradeAggregate a, TradeAggregate b) {
        a.sumTradeVolume += b.sumTradeVolume;
        a.sumTradeAmount += b.sumTradeAmount;
        a.tradeCount += b.tradeCount;
        return a;
    }
}
