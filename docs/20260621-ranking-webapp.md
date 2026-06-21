# Ranking WebApp

- **날짜**: 2026-06-21
- **작성자**: YoonRyeol

## 개요

Flink Job의 RankingResult를 실시간으로 시각화하는 웹 대시보드.

## 아키텍처

```
Flink TaskManager
  └── HttpRankingSink (POST /api/ranking)
        ↓ HTTP
ranking-webapp Pod (Flask, port 8080)
  ├── POST /api/ranking  → in-memory 저장
  ├── GET  /api/ranking  → JSON 반환 (AJAX용)
  └── GET  /             → HTML leaderboard
        ↓ port-forward
로컬 브라우저 http://localhost:8080
```

## 컴포넌트

### Flink HttpRankingSink
- `RankingSink` 인터페이스: 추후 KafkaSink 등으로 교체 가능
- `HttpRankingSink`: POST 요청으로 웹앱에 데이터 전송
- 실패 시 WARN 로그만 남기고 skip (Flink job 중단 방지)

### WebApp (Python Flask)
- `POST /api/ranking` — Flink로부터 ranking 데이터 수신
- `GET /api/ranking` — 현재 ranking JSON 반환
- `GET /` — HTML leaderboard 페이지
- In-memory 저장 (최신 1개 window만 유지)
- 스레드 안전 (threading.Lock)

### Leaderboard UI
- 순위, code, 거래량, 거래대금, metric 표시
- 5초 주기 auto-refresh (`<meta http-equiv="refresh">`)
- 1/2/3위 gold/silver/bronze 색상

## 배포

| 리소스 | 위치 |
|---|---|
| 소스코드 | `webapp/app.py` |
| Dockerfile | `webapp/Dockerfile` |
| K8s 매니페스트 | `k8s/webapp.yaml` |
| 이미지 | `ghcr.io/yoonryeol/ranking-webapp:latest` |

## 접속 방법

```bash
# port-forward
KUBECONFIG=~/.kube/config-k3s kubectl port-forward -n bitcoin-realtime-streaming \
  svc/ranking-webapp 8080:8080

# 브라우저에서
open http://localhost:8080
```

## 향후 개선

- NodePort 또는 Ingress로 외부 오픈
- PostgreSQL 저장소로 업그레이드 (히스토리 조회)
- WebSocket으로 실시간 갱신
