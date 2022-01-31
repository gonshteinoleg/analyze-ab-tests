import pandas as pd
import time
from google.cloud import bigquery
from datetime import datetime, timedelta
import numpy as np
import math as mth
from scipy import stats as st
import plotly.express as px
import warnings
warnings.filterwarnings('ignore')

client = bigquery.Client.from_service_account_json(
    'secret-file.json')

# get stat about transactions in groups
query = '''
WITH 
-- except dealers
more_three_transactions AS
(
SELECT fullVisitorId, transactions
FROM
  (
  SELECT fullVisitorId, COUNT(DISTINCT CONCAT(hits_time, fullVisitorId)) as transactions
  FROM `project-name.CommonData.transactions`
  WHERE date BETWEEN '2021-12-13' AND '2022-01-19' AND transactionType NOT IN ('business', 'offer')
  GROUP BY 1
  )
WHERE transactions > 3
),
test_groups AS
(
  SELECT date, visitId, fullVisitorId, 
  CASE
    WHEN user_separator IN ('part5', 'part6', 'part7', 'part8')
    THEN 0
    WHEN user_separator IN ('part9', 'part10', 'part11', 'part12')
    THEN 1
    WHEN user_separator IN ('part1', 'part2', 'part3', 'part4')
    THEN 2
  END as test_group
  FROM `project-name.CommonData.hits` 
  WHERE date BETWEEN '2022-01-13' AND '2022-01-19' 
  AND pagePath LIKE "%/shop/number%"
  AND fullVisitorId NOT IN (
      SELECT fullVisitorId
      FROM more_three_transactions
  )
),
transactions_table AS 
(
  SELECT date, visitId, fullVisitorId, COUNT(DISTINCT CONCAT(hits_time, fullVisitorId)) as transactions
  FROM `project-name.CommonData.transactions`
  WHERE date BETWEEN '2022-01-13' AND '2022-01-19' AND transactionType NOT IN ('business', 'offer')
  AND fullVisitorId NOT IN (
      SELECT fullVisitorId
      FROM more_three_transactions
      )
  GROUP BY 1, 2, 3
)
SELECT a.date, a.test_group, users, transactions
FROM
(
  SELECT date, test_group, COUNT(DISTINCT fullVisitorId) as users
  FROM test_groups
  GROUP BY 1, 2
) as a
JOIN
(
  SELECT a.date, test_group, SUM(transactions) as transactions
  FROM
    (
    SELECT date, visitId, fullVisitorId, test_group
    FROM test_groups
    ) as a
  JOIN
    (
    SELECT date, visitId, fullVisitorId, transactions
    FROM transactions_table
    ) as b on a.visitId=b.visitId AND a.fullVisitorId=b.fullVisitorId
  GROUP BY 1, 2
) as b on a.date=b.date AND a.test_group=b.test_group
ORDER BY date
'''

purchase = client.query(query, project='project-name').to_dataframe()

purchase['conversion'] = purchase['transactions'] / purchase['users'] * 100

group_a = purchase.query('test_group == 0')['conversion']
group_b = purchase.query('test_group == 2')['conversion']
alpha = .05
results = st.mannwhitneyu(group_a, group_b)
print('p-значение: ', results.pvalue)

if (results.pvalue < alpha):
    print("Разница статистически значима")
else:
    print("Нет оснований считать выборки разными")

a = (purchase.query('test_group == 0')['transactions'].sum() /
     purchase.query('test_group == 0')['users'].sum() * 100)
b = (purchase.query('test_group == 2')['transactions'].sum() /
     purchase.query('test_group == 2')['users'].sum() * 100)
uplift = (b - a) / a * 100

print("Конверсия в добавление в корзину в контрольной группе:", a)
print("Конверсия в добавление в корзину в тестовой группе:", b)
print("uplift:", uplift)

px.line(purchase, x="date", y="conversion", color='test_group')
