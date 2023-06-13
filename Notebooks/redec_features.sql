with base as (
select distinct biz.business_id, date(current_timestamp) as run_date, min(due_date - INTERVAL '1 month') over(partition by biz.business_id) as min_created_at
from LENDING_PAYMENT_SCHEDULES p
join LENDING_BUSINESSES biz on p.lending_business_id = biz.id
where
-- business_id in ('$business_id')
business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- and due_date < date(current_timestamp)

)

,txns_post_lending as (
select *,
sum(case when daysFromDueDate <= 60 and type = 'debit' then -amount else 0 end) over (partition by business_id) as amount_debited_2m,
sum(case when daysFromDueDate <= 60 and type = 'credit' then amount else 0 end) over (partition by business_id) as amount_credited_2m,
sum(case when daysFromDueDate <= 30 then ach_credit_txn else 0 end) over (partition by business_id) as count_ach_credit_1m,
sum(case when daysFromDueDate <= 30 then ach_debit_txn else 0 end) over (partition by business_id) as count_ach_debit_1m,
sum(case when daysFromDueDate <= 30 and type = 'credit' then 1 else 0 end) over (partition by business_id) as distinct_credit_txns_1m
from (
select  distinct pay.business_id, pay.run_date, t.transaction_date,pay.min_created_at, amount, type, t.status, running_balance, t.id as transaction_id,
EXTRACT(DAY FROM TRANSACTION_DATE::timestamp - pay.min_created_at::timestamp) as daySinceLoanTaken,
EXTRACT(DAY FROM pay.run_date::timestamp - TRANSACTION_DATE::timestamp) as daysFromDueDate,
CASE WHEN TYPE ='debit' AND MEDIUM  ILIKE ANY(array['%%external withdrawal%%', '%%iat withdrawal%%']) THEN 1 ELSE 0 END AS ach_debit_txn,
CASE WHEN type ='credit' AND medium ILIKE ANY(array['%%external deposit%%', '%%iat deposit%%']) THEN 1 ELSE 0 END AS ach_credit_txn
FROM base pay
left join TRANSACTIONS t on t.business_id = pay.business_id
where t.status = 'active'
and pay.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- and pay.business_id in ('$business_id')
and EXTRACT(DAY FROM TRANSACTION_DATE::timestamp - pay.min_created_at::timestamp) <= 180
and EXTRACT(DAY FROM TRANSACTION_DATE::timestamp - pay.min_created_at::timestamp) >= 0
and EXTRACT(DAY FROM pay.run_date::timestamp - TRANSACTION_DATE::timestamp) >= 0
) a
)
,txns_with_categories AS (
select *,
CASE WHEN description_processed ILIKE any(array[
                                                '%%novo funding%%',
			            						'%% earninactivehours %%',
                                                'earninactivehours %%',
                                                '%% earninactivehours',
                                                'earninactivehours',
			            						'%% activehours %%',
                                                'activehours %%',
                                                '%% activehours',
                                                'activehours',
			            						'%% navchek %%',
                                                'navchek %%',
                                                '%% navchek',
                                                'navchek',
			            						'%% loan %%',
                                                'loan %%',
                                                '%% loan',
                                                'loan',
			            						'%% earnin %%',
                                                'earnin %%',
                                                '%% earnin',
                                                'earnin',
			            						'%% advance %%',
                                                '%% advance',
                                                'advance %%',
                                                'advance']
			            ) THEN 1 ELSE 0 END AS LOANS,
			            CASE WHEN description_processed ILIKE any(
			            						array['%% hcclaimpmt %%',
                                                '%% hcclaimpmt',
                                                'hcclaimpmt %%',
                                                'hcclaimpmt',
												 '%% unitedhealthcare %%',
                                                 'unitedhealthcare %%',
                                                 '%% unitedhealthcare',
                                                 'unitedhealthcare',
												 '%% bcbs %%',
                                                 '%% bcbs',
                                                 'bcbs %%',
                                                 'bcbs',
												 '%% cigna %%',
                                                 'cigna %%',
                                                 '%% cigna',
                                                 'cigna',
												 '%% govt %%',
                                                 '%% govt',
                                                 'govt %%',
                                                 'govt',
												 '%% humana %%',
                                                 '%% humana',
                                                 'humana %%',
                                                 'humana',
												 '%% aetna %%',
                                                 'aetna %%',
                                                 '%% aetna',
                                                 'aetna',
												 '%% insurance %%',
                                                 'insurance %%',
                                                 '%% insurance',
                                                 'insurance',
												 '%% life %%',
                                                 'life %%',
                                                 '%% life',
                                                 'life',
												 '%% blue %%',
                                                 '%% blue',
                                                 'blue %%',
                                                 'blue']
						) THEN 1 ELSE 0 END AS INSURANCE,
						CASE WHEN description_processed ILIKE any(
												array['%% sbtpg %%',
                                                '%% sbtpg',
                                                'sbtpg %%',
                                                'sbtpg',
												'%% xxtaxeip %%',
                                                'xxtaxeip %%',
                                                '%% xxtaxeip',
                                                'xxtaxeip',
												'%% tax %%',
                                                'tax %%',
                                                '%% tax',
                                                'tax',
												'%% irs %%',
                                                '%% irs',
                                                'irs %%',
                                                'irs',
												'%% treas %%',
                                                '%% treas',
                                                'treas %%',
                                                'treas',
												'%% taxrfd %%',
                                                '%% taxrfd',
                                                'taxrfd %%',
                                                'taxrfd']
						) THEN 1 ELSE 0 END AS TAX
from (
select a.business_id, run_date,
(date_part('month', pay.run_date::date) - date_part('month', TRANSACTION_DATE::date)) as txn_month, amount, description,
			REGEXP_REPLACE(REGEXP_REPLACE(lower(description), ':',' '), '[^a-z ]', '') AS description_processed, transaction_date
from transactions a
inner join base pay
on a.business_id = pay.business_id
		   WHERE (date_part('month', pay.run_date::date) - date_part('month', TRANSACTION_DATE::date)) < 2
		   and (date_part('month', pay.run_date::date) - date_part('month', TRANSACTION_DATE::date)) >= 0
           AND status='active' AND TYPE = 'credit'
          and pay.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
        --   and pay.business_id in ('$business_id')
)a
)
,all_credits AS (
SELECT a.BUSINESS_ID,txn_month,
               sum(amount)  AS total_credit_amount,
               sum(CASE WHEN LOANS = 1 THEN amount ELSE 0 end) AS total_loan_credit_amount,
               sum(CASE WHEN INSURANCE = 1 THEN amount ELSE 0 END) AS total_insurance_credit_amount,
               sum(CASE WHEN TAX = 1 THEN amount ELSE 0 end) AS total_tax_credit_amount
FROM  txns_with_categories a
GROUP BY 1,2
)
,all_txn_credits AS (
SELECT business_id,txn_month,
               COALESCE(total_credit_amount, 0) AS total_credit_amount,
               COALESCE(total_loan_credit_amount, 0) AS total_loan_credit_amount,
               COALESCE(total_insurance_credit_amount, 0) AS total_insurance_credit_amount,
               COALESCE(total_tax_credit_amount, 0) AS total_tax_credit_amount
FROM all_credits
)
,last_2mnth_revenue as (
SELECT business_id,
       CASE WHEN txn_month = 0 THEN 'past_1month_rev'
            WHEN txn_month = 1 THEN 'past_2month_rev'
            WHEN txn_month = 2 THEN 'past_3month_rev'
            WHEN txn_month in (4,5,6) THEN 'past_furthest_3months'
       END as month,
       ((total_credit_amount-total_loan_credit_amount-total_insurance_credit_amount-total_tax_credit_amount)
	           ) AS revenue
FROM all_txn_credits
where txn_month <= 1)

,rev_ratio as (
select business_id, revenue_past_1m, revenue_past_2m, revenue_past_1m/(1 + revenue_past_2m) as ratio_revenue_1m_2m from (
select business_id, revenue as revenue_past_1m, lead(revenue) over(order by business_id, month) as revenue_past_2m,
row_number() over(partition by business_id order by month) rnk
from (
select business_id, month, sum(revenue) revenue
from last_2mnth_revenue
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
group by 1,2
order by 1,2
)a
)b
where rnk = 1
)
,mom as (
select business_id, avg(amount_drawn_1m_prev) as amount_drawn_1m_prev, avg(amount_drawn_2m_prev) as amount_drawn_2m_prev,
avg(ratio_amt_drawn_mom) as ratio_amt_drawn_mom, avg(ratio_amt_cashin_mom) as ratio_amt_cashin_mom, avg(distinct_cashins_1m_prev) as distinct_cashins_1m_prev
from (
select *, amount_drawn_1m_prev/(amount_drawn_2m_prev + 1) as ratio_amt_drawn_mom, amount_cashin_1m_prev/(amount_cashin_2m_prev + 1) as ratio_amt_cashin_mom from (
select pay.business_id, (date_part('month', run_date::date) - date_part('month', b.created_at::date)) as txn_month,
sum(case when (date_part('month', run_date::date) - date_part('month', b.created_at::date)) = 0 and type = 'draw' then amount/100 else 0 end) over(partition by b.lending_business_id) as amount_drawn_1m_prev,
sum(case when (date_part('month', run_date::date) - date_part('month', b.created_at::date)) = 1 and type = 'draw' then amount/100 else 0 end) over(partition by b.lending_business_id) as amount_drawn_2m_prev,
sum(case when (date_part('month', run_date::date) - date_part('month', b.created_at::date)) = 0 and type = 'cash_in' then amount/100 else 0 end) over(partition by b.lending_business_id) as amount_cashin_1m_prev,
sum(case when (date_part('month', run_date::date) - date_part('month', b.created_at::date)) = 1 and type = 'cash_in' then amount/100 else 0 end) over(partition by b.lending_business_id) as amount_cashin_2m_prev,
count(case when (date_part('month', run_date::date) - date_part('month', b.created_at::date)) = 0 and type = 'cash_in' then trace_number end) over(partition by b.lending_business_id) as distinct_cashins_1m_prev

from base pay
left join  LENDING_TRANSACTIONS b on pay.business_id = b.business_id
where (date_part('month', run_date::date) - date_part('month', b.created_at::date)) >= 0
and (date_part('month', run_date::date) - date_part('month', b.created_at::date)) < 2
and pay.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- and pay.business_id in ('$business_id')
)a
) b
group by 1
)

,cal_dates_1 AS (
select generate_series(
   (date (current_date - INTERVAL '180 day'))::timestamp,
   (date (current_date))::timestamp,
   interval '1 day'
) as CAL_DATE
),
biz_dates_1 AS (
			SELECT a.BUSINESS_ID, MIN(a.transaction_date) AS FIRST_TXN_DATE
			from TRANSACTIONS  a
			where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- 			where business_id in ('$business_id')
			GROUP BY a.BUSINESS_ID),
PILOT_BIZ_DATES_TEMP AS (
		SELECT *
		FROM  cal_dates_1 A LEFT JOIN
		biz_dates_1 B ON
		date(A.CAL_DATE) >= date(B.FIRST_TXN_DATE)
		ORDER BY CAL_DATE ASC)
,PILOT_BIZ_TXN_TEMP AS (
		SELECT   Business_id ,TRANSACTION_DATE,running_balance
		FROM
		        (SELECT *, RANK () OVER (PARTITION BY BUSINESS_ID,TRANSACTION_DATE ORDER BY TRANSACTION_DATE DESC) RANKS
		         FROM TRANSACTIONS
                 where 1=1
                 and status='active'
                 and business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
                --  and business_id in ('$business_id')
		         )a
		WHERE RANKS=1
		order by Business_id, TRANSACTION_DATE)
,PILOT_DAILY_BALANCES_1  AS (
		SELECT BUSINESS_ID,FIRST_TXN_DATE, CAL_DATE,TRANSACTION_DATE,RUNNING_BALANCE,
		first_value(RUNNING_BALANCE) OVER (PARTITION BY BUSINESS_ID,GROUPER ORDER BY cal_date asc, TRANSACTION_DATE desc, RUNNING_BALANCE asc nulls last) as RUNNING_BALANCE_2
		FROM (
		SELECT A.BUSINESS_ID,A.CAL_DATE,A.FIRST_TXN_DATE,
		        B.TRANSACTION_DATE,B.RUNNING_BALANCE,
		         COUNT(running_balance) OVER (PARTITION BY a.BUSINESS_ID ORDER BY cal_date asc) as grouper
		        FROM PILOT_BIZ_DATES_TEMP A LEFT JOIN
		        PILOT_BIZ_TXN_TEMP B ON
		        A.BUSINESS_ID=B.BUSINESS_ID AND
		        date(A.CAL_DATE)= date(B.TRANSACTION_DATE)
		        ORDER BY A.CAL_DATE ASC)a
		ORDER BY BUSINESS_ID,CAL_DATE)
,final_daily_running_balance AS (
		 SELECT distinct a.business_id, b.run_date, EXTRACT(DAY FROM run_date::timestamp - a.cal_date::timestamp) as daysFromDueDate, cal_date, running_balance_2 AS running_balance_daily
		           FROM  PILOT_DAILY_BALANCES_1  a join txns_post_lending b
		           on a.business_id = b.business_id
		          WHERE EXTRACT(DAY FROM run_date::timestamp - a.cal_date::timestamp) > 0 AND EXTRACT(DAY FROM run_date::timestamp - a.cal_date::timestamp) <= 180
		          and b.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
		          --and b.business_id in ('$business_id')
		ORDER BY a.business_id)

,second_last_month_rb as (
				SELECT business_id,
				               AVG(running_balance_daily) as Avg_running_balance_2nd_m,
  							   Median(running_balance_daily) as Median_running_balance_2nd_m,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_2nd_m
				FROM final_daily_running_balance
				WHERE daysFromDueDate <=60 and daysFromDueDate > 30
				GROUP BY 1)
,feat as (
select distinct a.business_id, run_date, b.median_running_balance_2nd_m,
median(case when daysFromDueDate <= 60 and type = 'debit' then -amount else 0 end) over (partition by a.business_id) as median_amount_debited_2m,
sum(case when daysFromDueDate <= 30 and type = 'debit' then 1 else 0 end) over (partition by a.business_id) as distinct_debit_txns_1m,
amount_debited_2m/(amount_credited_2m + 1) as ratio_debit_credit_2m,
count_ach_credit_1m/(distinct_credit_txns_1m + 1) as ratio_ach_credit_freq_total_credit_1m,
ratio_amt_drawn_mom,
count_ach_debit_1m/(count_ach_credit_1m + 1) as ratio_ach_debit_credit_freq_1m,
ratio_amt_cashin_mom,
ratio_revenue_1m_2m,
distinct_cashins_1m_prev

from txns_post_lending a left join second_last_month_rb b
on a.business_id = b.business_id
left join mom m on a.business_id = m.business_id
left join rev_ratio r on a.business_id = r.business_id
where a.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where a.business_id in ('$business_id')
)

, fillna as (
select business_id, run_date,
case when median_running_balance_2nd_m is null then 971.005 else median_running_balance_2nd_m end as median_running_balance_2nd_m,
case when median_amount_debited_2m is null then 11.59 else median_amount_debited_2m end as median_amount_debited_2m,
case when distinct_debit_txns_1m is null then 40 else distinct_debit_txns_1m end as distinct_debit_txns_1m,
case when ratio_debit_credit_2m is null then 1.021660 else ratio_debit_credit_2m end as ratio_debit_credit_2m,
case when ratio_ach_credit_freq_total_credit_1m is null then 0.522774 else ratio_ach_credit_freq_total_credit_1m end as ratio_ach_credit_freq_total_credit_1m,
case when ratio_amt_drawn_mom is null then 0.0 else ratio_amt_drawn_mom end as ratio_amt_drawn_mom,
case when ratio_ach_debit_credit_freq_1m is null then 0.75 else ratio_ach_debit_credit_freq_1m end as ratio_ach_debit_credit_freq_1m,
case when ratio_amt_cashin_mom is null then 1.161163 else ratio_amt_cashin_mom end as ratio_amt_cashin_mom,
case when ratio_revenue_1m_2m is null then 0.998223 else ratio_revenue_1m_2m end as ratio_revenue_1m_2m,
case when distinct_cashins_1m_prev is null then 1.0 else distinct_cashins_1m_prev end as distinct_cashins_1m_prev
from feat
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)

,capping as (
select business_id, run_date,
case when median_running_balance_2nd_m > 28671.967000 then 28671.967000 when median_running_balance_2nd_m < -240.9432 then -240.9432 else median_running_balance_2nd_m end as median_running_balance_2nd_m,
case when median_amount_debited_2m > 263.691700 then 263.691700 when median_amount_debited_2m < 0.0 then 0.0 else median_amount_debited_2m  end as median_amount_debited_2m,
case when distinct_debit_txns_1m > 815.000000 then 815.000000 when distinct_debit_txns_1m < 1 then 1 else distinct_debit_txns_1m  end as distinct_debit_txns_1m,
case when ratio_debit_credit_2m > 3.647596 then 3.647596 when ratio_debit_credit_2m < 0.4622 then 0.4622 else ratio_debit_credit_2m  end as ratio_debit_credit_2m,
case when ratio_ach_credit_freq_total_credit_1m > 0.963411 then 0.963411 when ratio_ach_credit_freq_total_credit_1m < 0 then 0 else ratio_ach_credit_freq_total_credit_1m  end as ratio_ach_credit_freq_total_credit_1m,
case when ratio_amt_drawn_mom > 8115.0 then 8115.0 when ratio_amt_drawn_mom < 0 then 0 else ratio_amt_drawn_mom  end as ratio_amt_drawn_mom,
case when ratio_ach_debit_credit_freq_1m > 11 then 11 when ratio_ach_debit_credit_freq_1m < 0 then 0 else ratio_ach_debit_credit_freq_1m  end as ratio_ach_debit_credit_freq_1m,
case when ratio_amt_cashin_mom > 4497.3 then 4497.3 when ratio_amt_cashin_mom < 0 then 0 else ratio_amt_cashin_mom  end as ratio_amt_cashin_mom,
case when ratio_revenue_1m_2m > 772.812175 then 772.812175 when ratio_revenue_1m_2m < 0 then 0 else ratio_revenue_1m_2m  end as ratio_revenue_1m_2m,
case when distinct_cashins_1m_prev > 12 then 12 when distinct_cashins_1m_prev < 0 then 0 else distinct_cashins_1m_prev  end as distinct_cashins_1m_prev
from fillna
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)

,scaler as (
select business_id, run_date,
(median_running_balance_2nd_m - 2695.882562)/4683.993809 as median_running_balance_2nd_m,
(median_amount_debited_2m - 22.615685)/40.766923 as median_amount_debited_2m,
(distinct_debit_txns_1m - 78.021832)/128.478779 as distinct_debit_txns_1m,
(ratio_debit_credit_2m - 1.115814)/0.413614 as ratio_debit_credit_2m,
(ratio_ach_credit_freq_total_credit_1m - 0.516212)/0.273880 as ratio_ach_credit_freq_total_credit_1m,
(ratio_amt_drawn_mom - 330.739217)/1200.548595 as ratio_amt_drawn_mom,
(ratio_ach_debit_credit_freq_1m - 1.296399)/1.769703 as ratio_ach_debit_credit_freq_1m,
(ratio_amt_cashin_mom - 261.157620)/745.312731 as ratio_amt_cashin_mom,
(ratio_revenue_1m_2m - 13.258101)/84.348046 as ratio_revenue_1m_2m,
(distinct_cashins_1m_prev - 2.009052)/1.905494 as distinct_cashins_1m_prev
from capping
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)


,coef as (
select business_id, run_date,
-0.47209342 + 0.42226452*distinct_debit_txns_1m - 0.74834938*median_amount_debited_2m + 0.28104661*ratio_debit_credit_2m
- 0.20423503*ratio_ach_credit_freq_total_credit_1m - 0.1675404*ratio_ach_debit_credit_freq_1m - 0.74254127*median_running_balance_2nd_m
- 0.09420364*ratio_revenue_1m_2m + 0.01370285*distinct_cashins_1m_prev - 0.17199712*ratio_amt_drawn_mom + 0.15967327*ratio_amt_cashin_mom as coef_eq
from scaler
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)


,proba as (
select business_id, run_date, EXP(coef_eq)/(1 + EXP(coef_eq)) as rds1_proba
from coef
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)

,score as (
select business_id, run_date, 1000*(1-rds1_proba) as rds1_score
from proba
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)

,bin as (
select business_id, run_date,
case when rds1_proba > 0.6 then 1
when rds1_proba > 0.46 and rds1_proba <= 0.6 then 2
when rds1_proba > 0.31 and rds1_proba <= 0.46 then 3
when rds1_proba > 0.08 and rds1_proba <= 0.31 then 4
else 5 end as rds1_bin
from proba
where business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
)

select a.business_id, a.run_date, rds1_proba, rds1_score, rds1_bin
from proba a join score b on a.business_id = b.business_id and a.run_date = b.run_date
join bin c on a.business_id = c.business_id and a.run_date = c.run_date
where a.business_id in ('d052f389-df14-4d66-be18-dd2bf0ec5161')
-- where business_id in ('$business_id')
