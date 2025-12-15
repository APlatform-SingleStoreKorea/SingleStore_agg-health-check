<h1 align="center">SingleStore HAProxy Routing Agent</h1>

<p align="center">
  HAProxy와 Python Sidecar를 활용한 <b>SingleStore 지능형 로드밸런싱(Smart Routing)</b> 구현체입니다.
</p>

<p align="center">
  <a href="https://a-platform.tistory.com/">
    <img src="https://img.shields.io/badge/Tistory-Blog-FF5722?style=for-the-badge&logo=tistory&logoColor=white"/>
  </a>
  <a href="https://blog.naver.com/a-platformbiz">
    <img src="https://img.shields.io/badge/Naver-Blog-03C75A?style=for-the-badge&logo=naver&logoColor=white"/>
  </a>
</p>

<p align="center">
  <img src="https://github.com/user-attachments/assets/4cebea19-bdb9-4eeb-ae78-a556d10218c7" width="600"/>
</p>
<br>

## 📖 Overview
이 프로젝트는 **HAProxy**가 SingleStoreDB 클러스터의 논리적 상태를 판단할 수 있도록 돕는 **Custom Health Check Agent**입니다.

단순한 포트 체크(TCP Check)만으로는 알 수 없는 DB 내부 상태(Master 역할, 파티션 상태 등)를 정밀하게 진단하여, DDL(데이터 정의)과 DML(데이터 조작) 트래픽을 올바른 노드로 자동 분산합니다.

## ✨ 주요 기능 (Key Features)
이 코드는 다음의 핵심 기능을 수행하여 무중단 운영 환경을 지원합니다.

- ✅ **DDL/DML 엔드포인트 분리:** - `Port 3306`: 오직 Master Aggregator(MA)로만 트래픽 라우팅 (DDL용)
  - `Port 3307`: 모든 **Healthy Aggregator**로 트래픽 부하 분산 (DML용)
- ✅ **Split-Brain 감지:** 네트워크 단절 시 Quorum(과반수) 그룹에 속해 있는지 검증
- ✅ **파티션 상태 점검:** 데이터 파티션 오프라인 시 트래픽 유입 자동 차단

## 🛠️ 아키텍처 (Architecture)

HAProxy는 `http-check` 옵션을 통해 Python Agent(Port 3309)와 통신하며 라우팅을 결정합니다.

| Port | Role | Routing Logic | Check Endpoint |
| :--- | :--- | :--- | :--- |
| **3306** | **DDL Endpoint** | Only **Master Aggregator** | `GET /ma_check` |
| **3307** | **DML Endpoint** | All **Healthy Nodes** | `GET /agg_check` |
