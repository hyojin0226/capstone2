# ADR 0008: 확신도 임계값과 `unknown` 판정 (7단계)

- 날짜: 2026-11-11
- 상태: **채택·구현 완료** (2026-11-11). 검증 결과: docs/confidence-threshold.md
- 관련: ADR 0003(Spark 추론, held-out 정확도), ADR 0007(집계 뷰, 급증 알림), docs/model-improvements.md(오프라인 실험)

## 맥락

- 플로우 단위 held-out 정확도는 약 0.72다. 전부 답하게 하면 넷 중 하나가 틀린다.
- 오프라인 실험(model-improvements.md)에서 가장 효과가 큰 것은 확신도 임계값이었다. 피처 추가(model-sequence-features.md)는 효과가 없었다.
  - 임계값 0.8: 플로우 75.6%, 바이트 98.3%에 답하고 정확도 0.827
  - 임계값 0.9: 플로우 62.2%, 바이트 93.8%에 답하고 정확도 0.861
- 파이프라인은 지금 argmax 결과를 그대로 클래스로 쓴다. 대시보드와 급증 알림은 확신이 낮은 예측까지 센다.

## 결정

1. **판정은 Spark에서 한다.** `pred_accepted = pred_confidence >= CONFIDENCE_THRESHOLD`이고, 추론할 때 쓴 임계값도 `confidence_threshold`에 같이 남긴다.
   - 임계값은 환경 변수 `CONFIDENCE_THRESHOLD`이고 기본값은 0.8이다. 바꾸려면 Spark를 재시작한다.
   - 비교는 float32로 저장한 확신도 기준으로 한다. 저장된 값으로 다시 계산해도 판정이 같다.
2. **예측 클래스는 지우지 않는다.** `pred_service_class`는 임계값과 무관하게 argmax로 남긴다.
   - 나중에 다른 임계값으로 다시 볼 수 있다. 오분류 분석도 계속할 수 있다.
   - `unknown`은 저장 값이 아니라 뷰(`flows_dedup.traffic_class`)에서 만든다.
3. **`unknown`은 다섯 서비스 클래스와 별개인 traffic_class다.** 모든 행이 정확히 하나의 클래스에 들어가므로 합계 일치 검증(ADR 0007)은 그대로 성립한다.
   - "분류된 트래픽" 패널과 급증 알림은 확신 예측만 센다(`unknown` 제외).
   - "전체 트래픽" 패널은 `unknown`을 따로 보인다.
4. **held-out 성능은 두 가지로 본다.**
   - `realtime_holdout_accuracy`(005): 모든 예측을 채점한다. 모델 자체의 성능이다.
   - `realtime_holdout_selective`(010): 운영 판정 기준이다. 답한 비율(플로우·바이트), 답한 것의 정확도, 전부 답했을 때의 정확도를 모델·임계값별로 보인다.
5. **기존 행은 "임계값 0 = 전부 수락"으로 이관한다.** `pred_accepted DEFAULT inferred`, `confidence_threshold DEFAULT 0`을 쓴다. 임계값 도입 전 데이터의 의미(argmax를 그대로 씀)가 바뀌지 않는다.
6. **기본값은 0.8로 한다.** 근거는 학습 시드 10개 평균이다.
   - 0.9는 정확도가 더 높다(0.861). 하지만 트래픽 바이트의 6%를 `unknown`으로 남긴다.
   - 0.8은 바이트 98%에 답하면서 정확도 0.83이라, 대시보드의 "유형별 트래픽 양"을 거의 그대로 유지한다.
   - 운영 모델 A의 held-out을 보고 다시 고르지 않는다. 고르면 그 held-out은 더 이상 처음 보는 데이터가 아니다(confidence-threshold.md 3절).

## 고려한 대안

| 대안 | 채택하지 않은 이유 |
|---|---|
| Spark에서 `pred_service_class`를 `unknown`으로 덮어쓰기 | 임계값을 바꾸거나 분석하려면 재추론해야 한다. 원래 예측 정보가 사라진다 |
| 임계값을 ClickHouse 뷰에만 두기 (설정 테이블 조인) | 큰 `flows_raw FINAL` 위에 조인이 하나 더 붙는다. 그리고 "그 행을 적재할 때 어떤 기준으로 판정했는가"가 남지 않는다 |
| 클래스별 임계값 | 클래스마다 세션 수가 2–28개라, 클래스별로 조정하면 held-out에 과적합하기 쉽다. 하나로 시작한다 |

## 결과·영향

- 스키마: `flows_raw`·`flow_predictions`에 두 컬럼을 추가했다(001·003·006). `flow_predictions_mv` 쿼리는 `MODIFY QUERY`로 바꿨다(006). 010에서 새 뷰를 만들었다.
  - 기존 설치와 새 설치 모두 같은 스키마 루프(backfill 제외)로 맞춰진다. 단, MV 쿼리를 바꾸는 동안에는 Spark를 멈춘다.
- dbt: `traffic_hourly.traffic_class`의 accepted_values에 `unknown`을 추가했다. `model_accuracy_daily`에 `answered`, `answered_correct`를 추가했다.
- 대시보드: 15개 패널(+1: held-out 답한 비율 / 답한 것의 정확도).
- 급증 알림은 확신 예측만 센다. 증폭한 트래픽 중 확신이 낮은 플로우는 세지 않으므로, 탐지 지연을 다시 쟀다(confidence-threshold.md).
