# Flink Job 배포 계획 (로컬 수동 배포)

> 본 문서는 `bitcoin-realtime-streaming-job` Flink 잡을 로컬 터미널에서 K3s 클러스터로 직접 배포하기 위한 절차를 정의합니다.
> GitOps(ArgoCD/Flux)나 GitHub Actions CD는 도입하지 않습니다.

## 1. 배경 및 범위

- **대상**: `bitcoin-realtime-streaming-job` Flink Application Mode 잡
- **배포 방식**: 로컬 터미널에서 수동으로 YAML apply
- **클러스터**: K3s 단일 노드(Hetzner CX23, 4GB RAM)
- **이미지 레지스트리**: GHCR (`ghcr.io/yoonryeol/bitcoin-realtime-streaming-job`)
- **비-GitOps 이유**: CX23 리소스 여유 제한, 배포 빈도 낮음, 단순성 우선

## 2. 사전 조건 (사용자가 준비/확인해야 할 항목)

### 2.1 K3s 클러스터 접근 정보
- [ ] K3s 서버 SSH 접속 정보 (또는 K3s kubeconfig)
- [ ] 로컬 `~/.kube/config`가 K3s 클러스터를 가리키도록 구성되어 있어야 함
  - 현재 kubeconfig는 회사 클러스터(`kcdl-prod-cd-v2`)를 가리키고 있어 K3s 접근 불가
  - K3s kubeconfig를 별도 파일로 두고 `KUBECONFIG=...` 환경변수로 전환 권장

### 2.2 GHCR 이미지 푸시 권한
- [ ] `docker login ghcr.io -u YoonRyeol` 완료 (GitHub PAT `write:packages` 권한 필요)
- [ ] `gh auth status` 정상 (계정: `YoonRyeol`)

### 2.3 GHCR 이미지 Pull 권한 (K3s 측)
- [ ] GHCR 패키지 가시성 확인 (producer는 public)
  - public인 경우: K3s에서 익명 pull 가능 → imagePullSecrets 불필요
  - private인 경우: K8s Secret + FlinkDeployment CRD의 `podTemplate.spec.imagePullSecrets` 추가 필요
- [ ] (private인 경우) `ghcr-pull-secret` Secret을 `bitcoin-realtime-streaming` namespace에 생성

### 2.4 클러스터 사전 구성
- [ ] namespace `bitcoin-realtime-streaming` 존재 여부 확인
- [ ] Flink K8s Operator 1.15.0 정상 동작 확인
- [ ] Kafka(`kafka.kafka.svc.cluster.local:9092`) 정상 동작 확인
- [ ] Kafka topic `upbit-trades` 존재 확인 (producer가 생성)

## 3. 빌드 파이프라인

### 3.1 JAR 빌드 (로컬)

```bash
cd bitcoin-realtime-streaming-job/main
./gradlew clean build
# 결과: build/libs/bitcoin-realtime-streaming-job-1.0-SNAPSHOT.jar
```

### 3.2 Docker 이미지 빌드

```bash
cd bitcoin-realtime-streaming-job/main
docker build -t ghcr.io/yoonryeol/bitcoin-realtime-streaming-job:latest -f infra/flink/Dockerfile .
```

### 3.3 GHCR 푸시

```bash
docker push ghcr.io/yoonryeol/bitcoin-realtime-streaming-job:latest
```

## 4. 배포 절차

### 4.1 namespace 생성 (최초 1회)

```bash
kubectl create namespace bitcoin-realtime-streaming
```

### 4.2 imagePullSecrets 생성 (private 이미지인 경우만)

```bash
kubectl create secret docker-registry ghcr-pull-secret \
  --namespace=bitcoin-realtime-streaming \
  --docker-server=ghcr.io \
  --docker-username=YoonRyeol \
  --docker-password=<GITHUB_PAT> \
  --docker-email=<EMAIL>
```

> 주의: PAT는 `read:packages` 권한만 있으면 충분.

### 4.3 FlinkDeployment apply

```bash
kubectl apply -f infra/flink/flinkdeployment.yaml
```

### 4.4 배포 상태 확인

```bash
# FlinkDeployment 상태
kubectl get flinkdeployment -n bitcoin-realtime-streaming -w

# Pod 상태
kubectl get pods -n bitcoin-realtime-streaming

# JobManager 로그
kubectl logs -n bitcoin-realtime-streaming -l app=bitcoin-realtime-streaming -c flink-job-manager --tail=200

# Flink Job 상태
kubectl exec -n bitcoin-realtime-streaming <jobmanager-pod> -- /opt/flink/bin/flink list
```

## 5. 업데이트 (이미지 갱신)

FlinkDeployment는 image tag가 `latest`로 고정되어 있으면 갱신을 감지하지 못함. 두 가지 방법 중 선택:

### 5.1 방법 A: tag 갱신 (추천)
GitHub Actions에서 이미지 tag를 `git-sha` 또는 타임스탬프로 생성 후 CRD의 image 필드 갱신.

```bash
# 로컬에서 수동으로 갱신할 경우
kubectl patch flinkdeployment bitcoin-realtime-streaming-job \
  --namespace=bitcoin-realtime-streaming \
  --type=json \
  -p='[{"op":"replace","path":"/spec/image","value":"ghcr.io/yoonryeol/bitcoin-realtime-streaming-job:<NEW_TAG>"}]'
```

### 5.2 방법 B: CRD 재적용
```bash
# YAML에서 image tag를 직접 수정한 후
kubectl apply -f infra/flink/flinkdeployment.yaml
```

### 5.3 방법 C: 잡 재시작 (stateless upgrade)
```bash
kubectl annotate flinkdeployment bitcoin-realtime-streaming-job \
  --namespace=bitcoin-realtime-streaming \
  force-restart=$(date +%s) --overwrite
```

## 6. 롤백

### 6.1 이전 이미지로 되돌리기

```bash
kubectl patch flinkdeployment bitcoin-realtime-streaming-job \
  --namespace=bitcoin-realtime-streaming \
  --type=json \
  -p='[{"op":"replace","path":"/spec/image","value":"ghcr.io/yoonryeol/bitcoin-realtime-streaming-job:<PREVIOUS_TAG>"}]'
```

### 6.2 잡 중단

```bash
kubectl delete flinkdeployment bitcoin-realtime-streaming-job -n bitcoin-realtime-streaming
```

## 7. 산출물 위치

```
side_project/bitcoin-realtime-streaming-job/main/
├── build.gradle.kts
├── settings.gradle.kts
├── src/main/java/io/github/yoonryeol/bitcoinrealtime/streaming/
│   ├── UpbitTradeJob.java              # Entry point
│   ├── UpbitTrade.java                 # Kafka deserialization
│   ├── TradeDedupFunction.java         # (code, sequential_id) dedup
│   ├── TradeAggregate.java             # Window aggregate POJO
│   ├── TradeAggregator.java            # AggregateFunction
│   ├── RankingEntry.java               # Rank entry POJO
│   ├── RankingResult.java              # Result POJO
│   ├── RankingCalculator.java          # Dense rank 계산 로직
│   └── RankingProcessFunction.java     # ProcessAllWindowFunction
├── src/test/java/...
└── infra/flink/
    ├── Dockerfile
    └── flinkdeployment.yaml
```

## 8. 검증 항목 (Smoke Test)

- [ ] JAR 빌드 성공 (`./gradlew clean build`)
- [ ] 단위 테스트 10개 통과
- [ ] Docker 이미지 빌드 성공
- [ ] GHCR 푸시 성공
- [ ] K3s 클러스터에서 `kubectl apply FlinkDeployment` 성공
- [ ] FlinkDeployment 상태 `RUNNING` 확인
- [ ] JobManager 로그에 Kafka consumer 연결 메시지 확인
- [ ] 표준 출력에 랭킹 결과 출력 확인 (1분 주기)

## 9. 비용

- **추가 인프라 비용**: 없음
- **클러스터 추가 리소스**: 없음
- **GitHub Actions 사용**: 미사용 (빌드는 로컬에서 실행)
- **GHCR 스토리지**: 무료 (public 패키지)

## 10. 제한 및 향후 검토

- **이미지 tag 관리 부담**: `:latest` 고정 시 자동 갱신 안 됨. 수동 patch 필요.
- **수동 kubectl 필요**: CD 자동화 없음. 사람이 직접 apply.
- **롤백 수동**: git revert로는 안 되고, 이전 tag로 patch해야 함.
- **향후 검토**:
  - 빌드 자동화(GitHub Actions CI만, push만) — producer와 동일 패턴
  - Flux/ArgoCD 도입 — CX32 업그레이드 후 재검토

## 11. AGENT.md 워크플로우 매핑

본 작업은 AGENT.md §2.0.1의 TDD 제외 대상:
- 단순 구성 파일(`flinkdeployment.yaml`) 수정
- 단순 빌드/배포 스크립트(Dockerfile) 실행

따라서:
- **TDD 미적용**: 별도 단위 테스트 작성 안 함
- **Smoke test로 동작 검증**: §8의 검증 항목으로 충당
- **사전 승인 필요**: K3s 클러스터 접근 및 `kubectl apply`는 파괴적 작업이므로 사용자 승인 후 진행

---

**작성일**: 2026-06-21
**대상 모델**: 코딩 에이전트 (Flink 잡 구현 + 배포 담당)