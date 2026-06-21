# [ADR] Apache Flink 기반 실시간 슬라이딩 Top-N 랭킹 파이프라인 설계서

- **날짜**: 2026-06-21
- **작성자**: YoonRyeol
- **주제**: 대용량 스트림 환경에서의 5분 슬라이딩 / 1분 주기 Top-N 랭킹 산출 아키텍처 결정
- **제약 조건**: 하류(Downstream) 데이터 스토리지가 Append-only(수정/UPSERT 불가) 환경

---

## 1. 아키텍처 진화 과정 (Step-by-Step)

### [STEP 1] 윈도우 연산 최적화 (Pre-aggregation)

**결정 사항:** Naïve한 `Sliding Window(5min, 1min)`를 쓰지 않고, **`1분 Tumbling Window` 선(先)집계 후 `5분 Sliding` 병합 구조**로 설계한다.

**의의:**
- 원본 이벤트 데이터를 1분 단위 숫자 1개로 압축하여 메모리에서 즉시 증발시킴.
- Flink 내부의 Pane 기반 최적화(Slicing Window)를 명시적으로 구현하여 State 크기와 연산량을 수십 분의 일로 절감함.
- 단, `SUM`, `COUNT` 같은 결합법칙 성립 연산에만 유효하며 `AVG`, `COUNT(DISTINCT)`에는 적용 불가.

### [STEP 2] Top-N 정렬을 위한 2단계 Keying 전략

**결정 사항:** `windowAll()`을 사용하지 않고, **`keyBy(windowEnd)` + `KeyedProcessFunction` 패턴**을 도입한다.

**의의:**
- `windowAll(1min)` 사용 시 전체 데이터가 단 하나의 Task 슬롯으로 몰리는 '병렬성 1의 저주(Bottleneck)'를 원천 차단.
- 레코드에 방출된 윈도우의 마감 시간(`windowEnd`)을 태그로 달고 이를 Key로 분산하여, **동시간대 데이터만 같은 Task 슬롯에 모아 정렬**하도록 설계.
- 정렬 완료 후 OOM 방지를 위해 반드시 `state.clear()`를 수행함.

### [STEP 3] 지각 데이터(Late Data)와 Append-only 극복 전략

**결정 사항:** **[실시간 Drop + Side Output 일배치 보정]** 아키텍처를 채택한다.

**의의:**
- `state.clear()` 이후 지각 데이터가 들어오면 지각생 단독 1위로 방출되는 **'유령 랭킹(Ghost Ranking)' 대형사고**가 발생함.
- 현재 DB가 Append-only이므로 유예기간을 준 정레이트(Re-emit) 덮어쓰기가 불가능.
- 따라서 1단계 윈도우에 `.allowedLateness(0)`을 부여해 **실시간 랭킹에서는 지각생을 가차 없이 버림** (랭킹의 핵심인 '스피드' 유지).
- 버려진 지각생은 Flink의 `Side Output`으로 빼서 Kafka `late-topic`에 모아두고, 새벽 4시에 도는 일배치 로직이 통계 DB의 총판매량 수치만 조용히 보정함.

### [STEP 4] '정시 발행(Wall-clock On-time)' 보장 전략

**결정 사항:** 비즈니스 요구사항(SLA)에 따라 아래 3가지 옵션 중 하나를 런타임에 선택한다.

1. **정합성 100% 우선 (EventTimeTimer):** 데이터 유입이 멈추면 랭킹 발행도 멈춤.
2. **정시성 100% 우선 (ProcessingTimeTimer):** 데이터 지연 상관없이 서버 Linux 시계 정각에 무조건 랭킹 컷.
3. **1티어 대형 B2C 표준 (Flink-Redis-Cron 분리):**
   - Flink는 정렬 타이머 없이 1분 윈도우 결과만 Redis `ZSET`에 쉴 새 없이 `ZADD`(Upsert)함.
   - 외부 API 서버가 OS Cron을 이용해 **정확히 매분 00.00초에** Redis를 긁어서 유저에게 방출.
   - Flink 클러스터가 GC로 멈춰도 유저 화면은 0.01초 오차 없이 갱신됨.

---

## 2. 스트림 엔진 코어 개념 요약표

| 구분 | Watermark Delay | Allowed Lateness |
|---|---|---|
| **핵심 역할** | 1차 발행을 위해 **"기다려주는"** 시간 | 1차 발행 후 수정을 위해 **"열어두는"** 시간 |
| **트리거 액션** | 윈도우 최초 발동 (**Fire & Emit**) | 지각 데이터 합산 후 **재발행 (Re-emit)** |
| **적용 대상** | 네트워크 랙 범위 내의 '정상 도착 데이터' | 워터마크 통과 이후 도착한 '진짜 지각생' |
| **본 설계 적용** | `Duration.ofSeconds(5)` 부여 | **`0` 부여 (Append-only 제약 때문)** |

---

## 3. 최종 데이터 파이프라인 모식도

```text
[Upbit WebSocket]
       ↓
[Python Producer] → Kafka topic "upbit-trades"
       ↓
[Flink Job]
  ├── Dedup (code + sequential_id, TTL 10min)
  ├── keyBy(code).window(Sliding 5min/1min).aggregate()
  │     └── ProcessWindowFunction → windowEnd 주입
  ├── keyBy(windowEnd).process(TopNRankingFunction)
  │     ├── ListState 적재
  │     ├── EventTimeTimer(windowEnd + 1ms)
  │     └── onTimer: 정렬 → 방출 → clear()
  ├── stdout (로그)
  └── HttpRankingSink → ranking-webapp (Flask)
                          ↓ port-forward
                    http://localhost:8080
```

---

## 4. 현재 설정값

| 파라미터 | 값 | 설명 |
|---|---|---|
| Window 크기 | 5분 | 집계 범위 |
| Window 슬라이드 | 1분 | 업데이트 주기 |
| Watermark delay | 5초 | 지연 데이터 허용 범위 |
| Allowed lateness | 0 | 재발행 없음 |
| Dedup TTL | 10분 | 중복 체결 제거 |
| Top N | 20 | 랭킹 출력 개수 |
| Checkpoint interval | 60초 | 장애 복구 주기 |

---

## 5. 향후 개선 로드맵

| Phase | 작업 | 우선순위 |
|---|---|---|
| 1 | 노드 증설 (CX32) + K3s 멀티노드 | 높음 |
| 2 | Prometheus + Grafana 모니터링 | 중간 |
| 3 | PostgreSQL 저장소 + 히스토리 대시보드 | 중간 |
| 4 | allowedLateness + Side Output 지각 데이터 처리 | 낮음 |
| 5 | Redis ZSET 기반 Wall-clock 발행 | 낮음 |
