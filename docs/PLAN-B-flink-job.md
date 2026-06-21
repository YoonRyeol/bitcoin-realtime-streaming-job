# B안 Flink Job 진행 계획서

> 상태: 2.3 구현 완료 · TC 10개 통과 · JAR 빌드 완료 · Dockerfile + FlinkDeployment YAML 작성 완료 · 2.4 Smoke test 대기
> 최종 업데이트: 2026-06-21
> 인프라 의존: `docs/PLAN-A-infra.md` (K3s + Kafka + Flink operator 배포 완료)
>
> **P0 결정 (2026-06-21)**
> - Dedup: `(code, sequential_id)` 키 KeyedProcessFunction + ValueState, state TTL ~10분. **1차 안정 동작 잡에 포함**.
> - TaskManager: limit **1.4Gi**, JVM heap **1.0Gi**, off-heap/native 400Mi.
> - 테스트 프레임워크: JUnit 5 (Jupiter) + Gradle `test` task + AssertJ (Flink `flink-test-utils` mini cluster로 Pipeline 테스트).
> - Flink 이미지: `flink:1.20.4` (Maven Central 최신 1.x stable, 2026-03-17 릴리스). CRD `flinkVersion: v1_20`. Operator 1.15.0 공식 지원.
>   - **참고**: Flink 2.1.x는 Maven Central에 artifact가 있으나 API 호환성 문제로 1.20.4 사용. Flink 2.x Docker 이미지는 2.1.2 사용 가능.
>
> **추가 결정 (2026-06-21)**
> - Sink: **stdout** (1차). Kafka sink는 추후 확장.
> - Top N: **20**.
> - 매수/매도: **매수 amount `+`, 매도 amount `-`** (부호 붙인 net flow).
> - Watermark: **event-time**, `boundedOutOfOrderness(Duration.ofSeconds(5))`.
> - OOM 유도 타이밍: **1차 안정 동작 확보 후 2차 별도 서브태스크** (나중에 설계).
> - worktree: 프로젝트명 **`bitcoin-realtime-streaming-job`**, 브랜치 **`main`**.
> - 빌드: **Gradle (Kotlin DSL)**, **JVM 17** (Flink 2.x 기본/권장. Java 21은 Flink 2.x에서 아직 experimental).

## 1. 목표

Kafka 토픽 `upbit-trades`의 업비트 체결 데이터를 Flink Application Mode로 consume하여, 종목 코드별 **5분 크기 / 1분 주기 sliding window** 거래량/거래대금 집계 랭킹을 실시간 산출. 이후 HashMap state backend에서 OOM을 유도하고 RocksDB로 전환하는 패턴을 검증.

## 2. 파이프라인 전체

```
Upbit WebSocket (wss://api.upbit.com/websocket/v1)
    ↓
Python Producer Pod (K3s 위, 이미 배포됨, 초당 ~8 msg/s 송신 중)
    ↓ produce JSON (핫스왑 창에서 일시적 중복 가능 → Flink dedup 대상)
Kafka topic "upbit-trades" (HelmForge Kafka, 이미 배포됨)
    ↓ consume
Flink Application Cluster (Java, 2차 작업에서 배포)
    ├── JobManager Pod (384Mi limit)
    └── TaskManager Pod (1Gi limit) ← OOM 유도 대상
    ↓ emit
Kafka topic "upbit-rankings-5min" (또는 stdout sink, 미결정)
```

## 3. 업비트 WebSocket Trade API 명세 요약

- **엔드포인트**: `wss://api.upbit.com/websocket/v1`
- **구독 요청 포맷** (JSON 배열):
  ```json
  [
    {"ticket": "<uuid>"},
    {"type": "trade", "codes": ["KRW-BTC","KRW-ETH"]},  // 또는 ["KRW-*"] 전 종목
    {"format": "DEFAULT"}
  ]
  ```
- **응답 스키마** (주요 필드):
  | 필드 | 의미 | 타입 |
  |---|---|---|
  | `type` | "trade" | String |
  | `code` | 페어 코드 (예: "KRW-BTC") | String |
  | `trade_price` | 체결 가격 | Double |
  | `trade_volume` | 체결량 | Double |
  | `ask_bid` | ASK(매도) / BID(매수) | String |
  | `prev_closing_price` | 전일 종가 | Double |
  | `change` | RISE/EVEN/FALL | String |
  | `change_price` | 전일 대비 변동 절대값 | Double |
  | `trade_date` | yyyy-MM-dd (UTC) | String |
  | `trade_time` | HH:mm:ss (UTC) | String |
  | `trade_timestamp` | 체결 타임스탬프 (ms) | Long |
  | `timestamp` | 수신 타임스탬프 (ms) | Long |
  | `sequential_id` | 체결 번호 (unique) | Long |
  | `best_ask_price/size` | 최우선 매도 호가/잔량 | Double |
  | `best_bid_price/size` | 최우선 매수 호가/잔량 | Double |
  | `stream_type` | SNAPSHOT / REALTIME | String |

> 공식 문서: https://docs.upbit.com/kr/reference/websocket-trade.md

## 4. Flink Job 설계안

### 4.1 Source
- **KafkaSource** (Flink Kafka Connector)
  - bootstrap: `kafka.kafka.svc.cluster.local:9092`
  - topic: `upbit-trades`, groupId: `bitcoin-realtime-streaming-job`
  - offsets: `OffsetsInitializer.latest()` (1차 신규 기동 시 backlog 재처리 방지)
  - deserializer: `JsonDeserializationSchema<UpbitTrade>` (POJO + Jackson `@JsonNaming(SnakeCaseStrategy)`)
- Watermark: **event-time**, `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner((t, prev) -> t.tradeTimestamp)` (업비트→핀란드 RTT ~50ms 대비 여유). allowed lateness = 0 (늦은 이벤트 drop)

### 4.2 Processing (파이프라인 shape)

```
KafkaSource(UpbitTrade)
  .assignTimestampsAndWatermarks(boundedOutOfOrderness(5s) ← tradeTimestamp)
  .keyBy(t -> Tuple2.of(t.code, t.sequentialId))
  .process(new TradeDedupFunction())            // 중복 제거, Output = UpbitTrade
  .keyBy(t -> t.code)
  .window(SlidingEventTimeWindows.of(5min, 1min))
  .aggregate(new TradeAggregator())             // AggregateFunction<UpbitTrade, TradeAggregate, TradeAggregate>
  .windowAll(SlidingEventTimeWindows.of(5min, 1min))
  .process(new RankingProcessFunction())       // Iterable<TradeAggregate> + Context → RankingResult
  .map(RankingResult::toString)
  .print()                                       // stdout sink
```

#### 0. Dedup — `TradeDedupFunction` (`KeyedProcessFunction<Tuple2<String,Long>, UpbitTrade, UpbitTrade>`)
- key: `Tuple2.of(code, sequentialId)`
- state: `ValueState<Boolean>` 이름 `"seen"`, `StateTtlConfig` 10분, **UpdateType.OnCreate** (첫 등록 시점부터 TTL)
- `processElement`: state null → `true` 저장 + `out.collect(trade)`; state true → skip. TTL 만료 후 재도달 → state null → 다시 emit (10분 내 중복만 제거. 핫스왑 창 ~ms이므로 안전)
- 목적: producer 설계 5.1 dual connection 핫스왑 창 중복 제거

#### 1-2. Keyed window — `keyBy(code)` + 5분/1분 sliding window
- 직전 5분 데이터 기반으로 **1분마다 랭킹 emit**. 각 trade는 ~5개 겹치는 window에 속해 state가 tumbling 대비 ~5배 (1차 안정 동작엔 문제 없고, 2차 OOM 유도엔 유리)

#### 3. Aggregate — `TradeAggregator` (`AggregateFunction<UpbitTrade, TradeAggregate, TradeAggregate>`)
- `add`: `accum.sumTradeVolume += t.tradeVolume`; `accum.sumTradeAmount += sign(t.askBid) * t.tradePrice * t.tradeVolume`; `accum.tradeCount++`
- `merge`: 필드별 합
- `getResult`: accum 반환
- **`sign(askBid)`**: `BID` → +1, `ASK` → -1 (Upbit 스펙상 두 값만. unknown은 스펙 외, 미고려)
- windowEnd는 이 단계에서 미포함 (랭킹 단에서 `windowAll` Context로 주입)

#### 4. Ranking — `RankingCalculator.rank(List<TradeAggregate>, windowStart, windowEnd) → RankingResult`
- `RankingProcessFunction` (`ProcessWindowFunction<TradeAggregate, RankingResult, String, TimeWindow>`)가 `Context.window()` 로 windowStart/End 획득 후 호출. 얄은 러퍼.
- metric = **`sumTradeVolume * sumTradeAmount`** (부호 유지 → 음수 가능)
- 정렬: metric **내림차순**, 동점 tie-break = code 오름차순 (표시 순서 보장용)
- **Dense rank**: 동점은 같은 rank, 다음 rank 건너뛰지 않음 (예: [100,100,90] → [1,1,2])
- **Top 20 = rank ≤ 20인 항목 전부** (동점 경계면 20개 초과 가능. float 곱이라 실제 동점은 거의 안 생김). rank 1-based
- **빈 입력(5분 내 trade 없음)** → 빈 `entries` 반환 → ProcessFunction emit 안 함

#### 5. 매수/매도 분리: `sumTradeAmount`에 부호(`+`/`-`)로 통합 반영. 별도 필드 분리는 2차 OOM 서브태스크에서 state 확장 시 검토.

### 4.3 Sink
- **stdout** (1차): `.map(RankingResult::toString).print()`. `RankingResult.toString()`은 사람이 읽기 쉬운 1줄 포맷(windowEnd ISO + 각 entry rank/code/vol/amount/metric/count). smoke test는 `kubectl logs`에서 `KRW-BTC` grep 등으로 검증.
- Kafka 토픽 `upbit-rankings-5min` emit은 추후 확장 (1차 범위 외)

### 4.4 State Backend (1차)
- `HashMapStateBackend` — 메모리에 state 보관 → OOM 유도 대상 (Flink 2.x: `state.backend.type: hashmap` 또는 programmatic `env.setStateBackend(new HashMapStateBackend())`)
- Checkpoint: 60s, `EXACTLY_ONCE`, storage `file:///opt/flink/checkpoints` (NVMe 로컬)
- RocksDB 전환은 OOM 재현 후 별도 서브태스크에서 진행

### 4.5 자원 (FlinkDeployment CRD)
- JobManager: 384Mi limit, heap 256Mi
- TaskManager: **1.4Gi limit, heap 1.0Gi**, off-heap/native 400Mi (이 안에서 HashMap state가 자라나 OOM 유도)
- Task slots: 1 (단일 TM)
- Image: `flink:1.20.4` (Maven Central 최신 1.x stable, 2026-03-17 릴리스. Flink 2.x API 호환성 문제로 1.20.4 사용)
- CRD `flinkVersion: v1_20` (Operator 1.15.0 공식 지원)

### 4.6 POJO / 타입 정의

| 타입 | 필드 | 비고 |
|---|---|---|
| `UpbitTrade` (입력) | `String code`, `double tradePrice`, `double tradeVolume`, `long tradeTimestamp(ms)`, `String askBid`, `long sequentialId`, `String streamType` | Flink POJO 규칙 준수 (public, no-arg ctor, public fields 또는 getter/setter). Jackson `@JsonNaming(PropertyNamingStrategies.SnakeCaseStrategy)` 로 스네이크 매핑. 그 외 Upbit 필드(`change`, `best_ask_*` 등)는 drop |
| `TradeAggregate` (accumulator 겸 출력) | `String code`, `double sumTradeVolume`, `double sumTradeAmount(부호)`, `long tradeCount` | window 필드 없음 |
| `RankingEntry` | `int rank(1-based)`, `String code`, `double sumTradeVolume`, `double sumTradeAmount`, `double metric(=vol*amount)`, `long tradeCount`, `long windowEnd(ms)` | |
| `RankingResult` | `long windowStart`, `long windowEnd`, `List<RankingEntry> entries` | `toString()` 1줄 포맷 |

### 4.7 Java 패키지
- `io.github.yoonryeol.bitcoinrealtime.streaming` (GitHub `YoonRyeol` 기반. 클래스: `UpbitTradeJob`, `UpbitTrade`, `TradeDedupFunction`, `TradeAggregator`, `TradeAggregate`, `RankingCalculator`, `RankingProcessFunction`, `RankingEntry`, `RankingResult`)

## 5. OOM 유도 전략 (별도 서브태스크)

1차엔 기본 5분/1분 sliding window로 안정 동작 먼저 확보. 이후 OOM 유도:
- 후보 A: state 보관 필드 확장 (최근 N윈도우 히스토리 보관, 종목별 micro-state 추가)
- 후보 B: 윈도우 크기 확장 (예: 1시간 크기 / 5분 주기 sliding) → state 폭증
- 후보 C: 일부러 checkpoint 비활성화하여 state 정리 안 되게
- 참고: sliding window 자체가 tumbling 대비 ~5배 state를 가지므로 OOM 유도에 이미 유리한 출발점
- OOM 재현 후 → `EmbeddedRocksDBStateBackend`로 전환 → NVMe 디스크 기반 복구 검증

> OOM 유도 전략은 안정 동작 확보 후 별도 설계(미결정 - 항목 6)

## 6. 배포 방식
- **FlinkDeployment CRD** (operator 관리, Application Mode)
- YAML 산출물 위치: `bitcoin-realtime-streaming-job/main/infra/flink/flinkdeployment.yaml`
- 패키징: **Gradle 빌드** → fat JAR (`shadowJar` 플러그인 등) → 커스텀 이미지 빌드 (또는 init container로 JAR 다운로드)
- 제출: `kubectl apply -f flinkdeployment.yaml` → operator가 JM/TM Pod 생성

## 7. TDD 워크플로우 적용 계획 (AGENT.md 2.x)

**테스트 프레임워크**: JUnit 5 (Jupiter) + Gradle `test` task + AssertJ. Pipeline/통합 테스트는 Flink `flink-test-utils` mini cluster 활용. (AGENT.md 2.0: 새 Java 프로젝트라 기존 Java 스택 없음 → 커뮤니티 표준으로 결정)

**빌드**: Gradle (Kotlin DSL), JVM 17 (Flink 1.20.4 기본/권장).

**의존성**:
- Flink Core: `flink-streaming-java:1.20.4`, `flink-clients:1.20.4`, `flink-runtime:1.20.4`
- Kafka Connector: `flink-connector-kafka:3.4.0-1.20`
- JSON: `flink-json:1.20.4`, `jackson-databind:2.18.2`
- Test: `junit-jupiter:5.11.4`, `flink-test-utils-junit:1.20.4`, `assertj-core:3.26.3`

| 산출물 | TDD 적용 | 비고 |
|---|---|---|
| Java Flink job 코드 (`TradeDedupFunction`, `TradeAggregator`, 랭킹 계산, POJO) | ✅ 대상 | JUnit 5 + AssertJ. 2.2 TC → 2.3 구현 |
| `FlinkDeployment` CRD YAML | ❌ 제외 | 단순 구성 파일 (AGENT.md 2.0.1) |
| Gradle `build.gradle.kts` / `settings.gradle.kts` | ❌ 제외 | 단순 구성 파일 |
| 컨테이너 이미지 빌드 스크립트 | ❌ 제외 | 단순 스크립트 |

## 8. 미결정 항목 (2.2 TC 작성 전 해결 필요)

1. ~~Sink 형태~~ → **stdout** (확정)
2. ~~랭킹 메트릭~~ → **`sumTradeVolume * sumTradeAmount`** (부호 유지, 음수 가능, **dense rank** 내림차순, rank ≤ 20, 동점 tie-break code asc, 빈 window emit 안 함) (확정)
3. ~~랭킹 Top N~~ → **20** (dense rank, rank ≤ 20) (확정)
4. ~~매수/매도 분리 집계~~ → **매수 `+`, 매도 `-` 부호로 amount에 통합 반영** (확정). 별도 필드 분리는 2차 OOM 서브태스크에서 검토.
5. ~~Watermark 전략~~ → **event-time, 5s** (확정)
6. ~~OOM 유도 타이밍~~ → **1차 안정 동작 확보 후 2차 별도 서브태스크** (확정, 나중에 설계)
7. ~~프로젝트명 + worktree 브랜치명~~ → **`bitcoin-realtime-streaming-job` / `main`** (확정)

## 9. 진행 상황 로그

- [x] 2026-06-20: 업비트 WebSocket Trade API 명세 확인 (공식 문서)
- [x] 2026-06-20: 2.1 설계 입력 초안 작성 (이 문서)
- [x] 2026-06-21: P0 3건 확정 (dedup `(code, sequential_id)` 1차 포함 / TM 1.4Gi heap 1.0Gi / JUnit 5 + Surefire + AssertJ) + Flink 이미지 `flink:2.1.2` 확정 (operator 1.15.0 호환 확인, 2.1.3은 Maven Central 미동기화로 2.1.2 사용)
- [x] 2026-06-21: 추가 결정 — Sink stdout / Top 20 / 매수+ 매도- / event-time 5s / OOM 2차 별도 / worktree `bitcoin-realtime-streaming-job`:`main` / Gradle(Kotlin DSL) + JVM 17 (Java 21은 Flink 2.x experimental)
- [x] 2026-06-21: 항목 2 랭킹 메트릭 확정 — `sumTradeVolume * sumTradeAmount` (부호 유지, dense rank 내림차순 Top 20, 빈 window emit 안 함)
- [x] 2026-06-21: 2.1 설계 구체화 — 파이프라인 shape / POJO 4종 타입 정의 / Dedup·Aggregator·Ranking 세부 동작 / KafkaSource·Checkpoint 설정 / Java 패키지 `io.github.yoonryeol.bitcoinrealtime.streaming` / 5분 크기 1분 주기 sliding window 반영
- [x] 2026-06-21: worktree 생성 `bitcoin-realtime-streaming-job/main/` (bare repo + origin + main 브랜치 초기 커밋 + push). Gradle 프로젝트 스캐포폴드 (Kotlin DSL, JVM 17, JUnit 5 + AssertJ + flink-test-utils-junit). Flink 버전 1.20.4 확정 (2.1.x API 호환성 문제).
- [x] 2026-06-21: Flink 1.20.4 API로 코드 전면 수정 완료. `UpbitTradeJob`, `TradeDedupFunction`, `TradeAggregator`, `RankingCalculator`, `RankingProcessFunction` 구현. `UpbitTradeDeserializationSchema` (KafkaRecordDeserializationSchema) 구현.
- [x] 2026-06-21: TC 10개 모두 통과 (RankingCalculatorTest 6개, TradeAggregatorTest 4개). Gradle build 성공.
- [x] 2026-06-21: Dockerfile + FlinkDeployment CRD YAML 작성 완료. JAR 빌드 완료 (36KB thin JAR).
- [x] 2.2 JUnit 5 TC 작성 및 사용자 승인
- [x] 2.3 Java Flink job 코드 구현 (TC 통과까지)
- [x] 패키징 (Gradle fat JAR + 컨테이너 이미지)
- [x] FlinkDeployment CRD YAML 작성
- [ ] 2.4 Smoke test (Pod 기동 + Kafka consumer lag + Flink Web UI job Running)
- [ ] OOM 유도 서브태스크 (별도 설계)
- [ ] RocksDB 전환 + 복구 검증

## 10. 산출물 예정 위치

```
side_project/
  docs/
    PLAN-A-infra.md                          (1차 인프라)
    PLAN-B-flink-job.md                      (이 문서)
  infra/
    kafka/values.yaml                         (1차 작업 완료)
  bitcoin-realtime-streaming-job/main/          ← AGENT.md §1 worktree 구조
    build.gradle.kts                           (예정, TDD 제외)
    settings.gradle.kts                        (예정, TDD 제외)
    gradle/                                    (예정, wrapper)
    src/
      main/java/io/github/yoonryeol/bitcoinrealtime/streaming/
        UpbitTradeJob.java                  (완료, 진입점 - TDD 제외)
        UpbitTrade.java                     (완료, POJO)
        TradeAggregate.java                 (완료, POJO)
        RankingEntry.java                   (완료, POJO)
        RankingResult.java                  (완료, POJO)
        TradeDedupFunction.java             (완료, 핫스왑 중복 제거)
        TradeAggregator.java                (완료, AggregateFunction)
        RankingCalculator.java              (완료, 순수 랭킹 로직)
        RankingProcessFunction.java         (완료, ProcessAllWindowFunction)
      test/java/io/github/yoonryeol/bitcoinrealtime/streaming/
        TradeAggregatorTest.java            (완료, 2.2 TC, 순수)
        RankingCalculatorTest.java          (완료, 2.2 TC, 순수)
    infra/flink/
      flinkdeployment.yaml                     (완료, TDD 제외)
      Dockerfile                               (완료, TDD 제외)
```

> worktree: `bitcoin-realtime-streaming-job/main/` (확정). producer repo `bitcoin-realtime-producer/main/` 패턴과 동일.