-- WAU (Weekly Active Users) 계산 쿼리
--
-- wau_by_user    : 주간 고유 user_id 수
-- wau_by_session : 주간 고유 generated_session_id 수 (항상 >= wau_by_user)
-- week_type      : PARTIAL = 경계 주(데이터 미완성), FULL = 7일 완전한 주
--
-- 단일 쿼리로 스캔 1회 + 셔플 1회 처리
-- Hive 2.3.2에서 date_trunc('WEEK') 미지원 → NEXT_DAY(DATE_SUB(dt, 7), 'MO') 로 대체
-- 검증: SELECT NEXT_DAY(DATE_SUB(TO_DATE('2019-10-02'), 7), 'MO') → 2019-09-30 (월요일) ✅

SELECT
    NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO')                  AS week_start,
    COUNT(DISTINCT user_id)                                    AS wau_by_user,
    COUNT(DISTINCT generated_session_id)                       AS wau_by_session,
    CASE
        WHEN NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO') < '2019-10-01'
          OR DATE_ADD(NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO'), 6) > '2019-12-01'
        THEN 'PARTIAL'
        ELSE 'FULL'
    END                                                        AS week_type
FROM ecommerce.events
GROUP BY NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO')
ORDER BY week_start;
