# WAU 쿼리

## user 기준 WAU와 session 기준 WAU를 함께 계산

user 기준 WAU와 session 기준 WAU를 따로 계산하면 같은 기간의 데이터를 두 번 스캔하게 됩니다.

```sql
-- 쿼리 1
SELECT week_start, COUNT(DISTINCT user_id) FROM ...

-- 쿼리 2
SELECT week_start, COUNT(DISTINCT generated_session_id) FROM ...
```

이 과제의 입력 데이터는 1억 건 이상이므로, 같은 테이블을 반복 스캔하지 않는 것이 중요합니다. 그래서 하나의 쿼리에서 두 지표를 함께 계산했습니다.

```sql
SELECT
    week_start,
    COUNT(DISTINCT user_id)              AS wau_by_user,
    COUNT(DISTINCT generated_session_id) AS wau_by_session
FROM ecommerce.events
GROUP BY week_start;
```

한 사용자가 여러 세션을 가질 수 있으므로 일반적으로 `wau_by_session >= wau_by_user` 관계가 성립합니다.

## Hive와 Athena의 주 시작일 처리

Athena 쿼리에서는 `date_trunc('WEEK', CAST(dt AS DATE))`를 사용해 주 시작일을 계산합니다.

```sql
SELECT
    date_trunc('WEEK', CAST(dt AS DATE))              AS week_start,
    COUNT(DISTINCT user_id)                           AS wau_by_user,
    COUNT(DISTINCT generated_session_id)              AS wau_by_session,
    CASE
        WHEN date_trunc('WEEK', CAST(dt AS DATE)) < DATE '2019-10-01'
          OR date_add('day', 6, date_trunc('WEEK', CAST(dt AS DATE))) > DATE '2019-12-01'
        THEN 'PARTIAL'
        ELSE 'FULL'
    END                                               AS week_type
FROM ecommerce.events
GROUP BY date_trunc('WEEK', CAST(dt AS DATE))
ORDER BY week_start;
```

로컬 Hive 2.3.2 환경에서는 `date_trunc('WEEK')` 지원이 제한적이어서, `sql/04_wau_query.sql`에서는 다음 표현으로 월요일 기준 주 시작일을 계산했습니다.

```sql
NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO')
```

이렇게 환경별 함수 차이를 반영해 로컬 Hive와 Athena 양쪽에서 WAU를 확인할 수 있도록 했습니다.

## 데이터 경계 처리

원본 데이터는 2019년 10월과 11월 로그입니다. 첫 주와 마지막 주는 전체 7일 데이터가 모두 들어 있지 않을 수 있습니다.

이런 경계 주를 단순히 제거하면 분석 결과가 왜곡될 수 있습니다. 예를 들어 특정 주의 WAU가 낮게 나왔을 때, 실제 유저가 줄어든 것인지 데이터가 일부 날짜만 포함된 것인지 구분하기 어렵습니다.

그래서 제거 대신 `week_type` 플래그를 제공합니다.

```sql
CASE
    WHEN week_start < '2019-10-01'
      OR DATE_ADD(week_start, 6) > '2019-12-01'
    THEN 'PARTIAL'
    ELSE 'FULL'
END AS week_type
```

분석 사용자는 `PARTIAL` 주를 제외할지, 참고용으로만 볼지 직접 판단할 수 있습니다.
