final_features = r"""
with base as (
select distinct biz.business_id, due_date, min(dateadd(month,-1,due_date)) over(partition by biz.business_id) as min_created_at from "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."LENDING_PAYMENT_SCHEDULES" p
left join "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."LENDING_BUSINESSES" biz on p.lending_business_id = biz.id
where due_date < date('2023-05-08')
)


,txns_post_lending as (
select  distinct pay.business_id, pay.due_date, t.transaction_date,pay.min_created_at, amount, type, t.status, running_balance, t.id as transaction_id,
datediff(day, pay.min_created_at, TRANSACTION_DATE) AS daySinceLoanTaken,
datediff(day, TRANSACTION_DATE, pay.due_date) AS daysFromDueDate,
CASE WHEN TYPE ='debit' AND MEDIUM  ILIKE ANY('%%external withdrawal%%', '%%iat withdrawal%%') THEN 1 ELSE 0 END AS ach_debit_txn,
CASE WHEN type ='credit' AND medium ILIKE ANY('%%external deposit%%', '%%iat deposit%%') THEN 1 ELSE 0 END AS ach_credit_txn,
sum(case when daysFromDueDate <= 60 and type = 'debit' then -amount else 0 end) over (partition by pay.business_id, due_date) as amount_debited_2m,
sum(case when daysFromDueDate <= 60 and type = 'credit' then amount else 0 end) over (partition by pay.business_id, due_date) as amount_credited_2m,
sum(case when daysFromDueDate <= 30 then ach_credit_txn else 0 end) over (partition by pay.business_id, due_date) as count_ach_credit_1m,
sum(case when daysFromDueDate <= 30 then ach_debit_txn else 0 end) over (partition by pay.business_id, due_date) as count_ach_debit_1m,
sum(case when daysFromDueDate <= 30 and type = 'credit' then 1 else 0 end) over (partition by pay.business_id, due_date) as distinct_credit_txns_1m
FROM base pay
-- left join "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."LENDING_BUSINESSES" biz on pay.business_id = biz.business_id
left join FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS" t on t.business_id = pay.business_id
where t.status = 'active'  AND  daySinceLoanTaken <=180 AND daySinceLoanTaken > 0 and daysFromDueDate > 0
-- and pay.lending_business_id = 'cf6d8b20-574f-4362-b1f5-274bb2231c05'
order by due_date
)

,txns_with_categories AS (
select a.business_id, pay.due_date,DATEDIFF(month, TRANSACTION_DATE, pay.due_date) AS txn_month, amount, description,
			REGEXP_REPLACE(REGEXP_REPLACE(lower(description), ':',' '), '[^a-z ]', '') AS description_processed, transaction_date,
			            CASE WHEN description_processed ILIKE any(
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
                                                'advance'
			            ) THEN 1 ELSE 0 END AS LOANS,
			            CASE WHEN description_processed ILIKE any(
			            						'%% hcclaimpmt %%',
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
                                                 'blue'
						) THEN 1 ELSE 0 END AS INSURANCE,
						CASE WHEN description_processed ILIKE any(
												'%% sbtpg %%',
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
                                                'taxrfd'
						) THEN 1 ELSE 0 END AS TAX
from fivetran_db.PROD_NOVO_API_PUBLIC.transactions a 
inner join base pay
on a.business_id = pay.business_id
		   WHERE datediff(MONTH ,TRANSACTION_DATE,pay.due_date) < 2 and datediff(MONTH ,TRANSACTION_DATE,pay.due_date) >= 0
           AND status='active' AND TYPE = 'credit'     
)
,all_credits AS (
SELECT a.BUSINESS_ID, due_date,txn_month,
               sum(amount)  AS total_credit_amount, 
               sum(CASE WHEN LOANS = 1 THEN amount ELSE 0 end) AS total_loan_credit_amount, 
               sum(CASE WHEN INSURANCE = 1 THEN amount ELSE 0 END) AS total_insurance_credit_amount, 
               sum(CASE WHEN TAX = 1 THEN amount ELSE 0 end) AS total_tax_credit_amount
FROM  txns_with_categories a
GROUP BY 1,2,3
),
all_txn_credits AS (
SELECT business_id, due_date,txn_month, 
               COALESCE(total_credit_amount, 0) AS total_credit_amount,
               COALESCE(total_loan_credit_amount, 0) AS total_loan_credit_amount,
               COALESCE(total_insurance_credit_amount, 0) AS total_insurance_credit_amount,
               COALESCE(total_tax_credit_amount, 0) AS total_tax_credit_amount
            --   COALESCE(TOTAL_CREDIT_AMOUNT_PLAID, 0) AS TOTAL_CREDIT_AMOUNT_PLAID,
            --   COALESCE(TOTAL_LOAN_CREDIT_AMT_PLAID, 0) AS TOTAL_LOAN_CREDIT_AMT_PLAID,
            --   COALESCE(TOTAL_INSURANCE_CREDIT_AMT_PLAID, 0) AS TOTAL_INSURANCE_CREDIT_AMT_PLAID,
            --   COALESCE(TOTAL_TAX_CREDIT_AMT_PLAID, 0) AS TOTAL_TAX_CREDIT_AMT_PLAID
FROM all_credits 
)
,last_2mnth_revenue as (
SELECT business_id, due_date,
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
select business_id, due_date, revenue_past_1m, revenue_past_2m, revenue_past_1m/(1 + revenue_past_2m) as ratio_revenue_1m_2m from (
select business_id,due_date, revenue as revenue_past_1m, lead(revenue) over(order by business_id, due_date, month) as revenue_past_2m,
row_number() over(partition by business_id, due_date order by month) rnk
from (
select business_id, due_date, month, sum(revenue) revenue
from last_2mnth_revenue
-- where business_id = '0d39a85e-12dc-4487-904a-55cc24c397bf'
group by 1,2,3
order by 1,2,3
)
)
where rnk = 1
)

,mom as (
select business_id, due_date, avg(amount_drawn_1m_prev) as amount_drawn_1m_prev, avg(amount_drawn_2m_prev) as amount_drawn_2m_prev, 
avg(ratio_amt_drawn_mom) as ratio_amt_drawn_mom, avg(ratio_amt_cashin_mom) as ratio_amt_cashin_mom, avg(distinct_cashins_1m_prev) as distinct_cashins_1m_prev
from (
select pay.business_id, pay.due_date, datediff(month, b.created_at, due_date) as txn_month,
sum(case when txn_month = 0 and type = 'draw' then amount/100 else 0 end) over(partition by b.lending_business_id, due_date) as amount_drawn_1m_prev,
sum(case when txn_month = 1 and type = 'draw' then amount/100 else 0 end) over(partition by b.lending_business_id, due_date) as amount_drawn_2m_prev,
sum(case when txn_month = 0 and type = 'cash_in' then amount/100 else 0 end) over(partition by b.lending_business_id, due_date) as amount_cashin_1m_prev,
sum(case when txn_month = 1 and type = 'cash_in' then amount/100 else 0 end) over(partition by b.lending_business_id, due_date) as amount_cashin_2m_prev,
count(distinct case when txn_month = 0 and type = 'cash_in' then trace_number end) over(partition by b.lending_business_id, due_date) as distinct_cashins_1m_prev,
amount_drawn_1m_prev/(amount_drawn_2m_prev + 1) as ratio_amt_drawn_mom,
amount_cashin_1m_prev/(amount_cashin_2m_prev + 1) as ratio_amt_cashin_mom
from base pay
left join  "FIVETRAN_DB"."PROD_NOVO_API_PUBLIC"."LENDING_TRANSACTIONS" b on pay.business_id = b.business_id
where txn_month >= 0 and txn_month <2
)
group by 1,2
)
,cal_dates_1 AS (
			SELECT DATEADD(DAY, SEQ4(), '2021-06-01') AS CAL_DATE
			 FROM TABLE(GENERATOR(ROWCOUNT=>960)) 
			 ORDER BY CAL_DATE DESC),
biz_dates_1 AS (
			SELECT a.BUSINESS_ID, MIN(a.transaction_date) AS FIRST_TXN_DATE
			from FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS"  a 
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
		         FROM FIVETRAN_DB.PROD_NOVO_API_PUBLIC."TRANSACTIONS" 
                 where 1=1
                 and status='active'
		         )
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
		        ORDER BY A.CAL_DATE ASC)
		ORDER BY BUSINESS_ID,CAL_DATE)
,final_daily_running_balance AS (
		 SELECT distinct a.business_id, b.due_date, datediff(day, a.cal_date, b.due_date) as daysFromDueDate, cal_date, running_balance_2 AS running_balance_daily
		           FROM  PILOT_DAILY_BALANCES_1  a join txns_post_lending b 
		           on a.business_id = b.business_id
		          WHERE daysFromDueDate > 0 AND daysFromDueDate <= 180
		ORDER BY a.business_id, daysFromDueDate)
				
,second_last_month_rb as (	
				SELECT business_id, due_date, 
				               AVG(running_balance_daily) as Avg_running_balance_2nd_m,
  							   Median(running_balance_daily) as Median_running_balance_2nd_m,
							   STDDEV(running_balance_daily) as STDDEV_running_balance_2nd_m
				FROM final_daily_running_balance
				WHERE daysFromDueDate <=60 and daysFromDueDate > 30
				GROUP BY 1,2)
-- select count(*) from (
select distinct a.business_id, a.due_date, b.median_running_balance_2nd_m, 
median(case when daysFromDueDate <= 60 and type = 'debit' then -amount else 0 end) over (partition by a.business_id, a.due_date) as median_amount_debited_2m,
sum(case when daysFromDueDate <= 30 and type = 'debit' then 1 else 0 end) over (partition by a.business_id, a.due_date) as distinct_debit_txns_1m,
amount_debited_2m/(amount_credited_2m + 1) as ratio_debit_credit_2m,
count_ach_credit_1m/(distinct_credit_txns_1m + 1) as ratio_ach_credit_freq_total_credit_1m,
ratio_amt_drawn_mom,
count_ach_debit_1m/(count_ach_credit_1m + 1) as ratio_ach_debit_credit_freq_1m,
ratio_amt_cashin_mom,
ratio_revenue_1m_2m,
distinct_cashins_1m_prev

from txns_post_lending a left join second_last_month_rb b
on a.business_id = b.business_id and a.due_date = b.due_date
left join mom m on a.business_id = m.business_id and a.due_date = m.due_date
left join rev_ratio r on a.business_id = r.business_id and a.due_date = r.due_date
-- where a.business_id = '1d33a47e-f784-48d5-b5d2-4febee80fbc2'
order by a.due_date

"""