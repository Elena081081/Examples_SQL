### ПОДЗАПРОСЫ

WITH t1 AS 

(SELECT product_id FROM products

ORDER BY price DESC 

LIMIT 5), 

t2 AS (SELECT order_id, unnest(product_ids) AS prod_id FROM orders), 

t3 AS (SELECT order_id FROM t2

WHERE prod_id IN (SELECT product_id FROM t1))

SELECT order_id, product_ids FROM orders

WHERE order_id IN (SELECT order_id FROM t3)

ORDER BY order_id

------------------------------

WITH avg_age AS 

(SELECT DATE_PART('year', AVG(age((SELECT MAX(time) FROM user_actions), birth_date))) AS age_user FROM users)

SELECT user_id, COALESCE(DATE_PART('year', age_user), (SELECT * FROM avg_age)) AS age

FROM (SELECT user_id, age((SELECT MAX(time) FROM user_actions), birth_date) AS age_user FROM users) t

ORDER BY user_id

### ОБЪДИНЕНИЕ ТАБЛИЦ (JOIN)

WITH t1 AS (SELECT order_id, unnest(product_ids) AS product_id FROM orders)

SELECT t1.order_id, ARRAY_AGG(p.name) AS product_names

FROM t1 JOIN products p ON t1.product_id = p.product_id

GROUP BY t1.order_id

LIMIT 1000

------------------------------

WITH 

t1 AS (SELECT MAX(ARRAY_LENGTH(product_ids, 1)) AS m_order FROM orders), 

t2 AS (SELECT MAX(time) AS m_time FROM user_actions)

SELECT orders.order_id, users.user_id, 

DATE_PART('year', age((SELECT m_time FROM t2), users.birth_date)) AS user_age, couriers.courier_id, 

DATE_PART('year', age((SELECT m_time FROM t2), couriers.birth_date)) AS courier_age

FROM orders 

JOIN courier_actions ON orders.order_id = courier_actions.order_id AND courier_actions.action LIKE '%deliv%' 

JOIN couriers ON courier_actions.courier_id = couriers.courier_id 

JOIN user_actions ON orders.order_id = user_actions.order_id AND user_actions.action LIKE '%create%' 

JOIN users ON user_actions.user_id = users.user_id

WHERE ARRAY_LENGTH(orders.product_ids, 1) = (SELECT m_order FROM   t1)

ORDER BY orders.order_id

------------------------------

WITH t1 AS 

(SELECT order_id FROM user_actions

WHERE action = 'cancel_order'), 

t2 AS 

(SELECT order_id, unnest(product_ids) AS product_id FROM orders

WHERE order_id NOT IN (SELECT order_id FROM t1)), 

t3 AS 

(SELECT t2.order_id, products.name FROM t2 JOIN products ON t2.product_id = products.product_id)

SELECT ARRAY[p1.name, p2.name] AS pair, COUNT(DISTINCT p1.order_id) AS count_pair

FROM t3 AS p1.order_id = p2.order_id AND p1.name < p2.name

GROUP BY p1.name, p2.name

ORDER BY count_pair DESC, pair

### ОКОННЫЕ ФУНКЦИИ

with t2 as (SELECT DISTINCT order_id,
                            creation_time,
                            sum(price) OVER(PARTITION BY order_id) as order_price,
                            sum(price) OVER(PARTITION BY creation_time::date) as daily_revenue
            FROM   (SELECT order_id,
                           creation_time,
                           unnest(product_ids) as product_id
                    FROM   orders) t1 join products p using(product_id)
            WHERE  order_id not in (SELECT order_id
                                    FROM   user_actions
                                    WHERE  action like '%cancel%'))
SELECT order_id,
       creation_time,
       order_price,
       daily_revenue,
       round(order_price/daily_revenue*100, 3) as percentage_of_daily_revenue
FROM   t2
ORDER BY date_trunc('day', creation_time) desc, percentage_of_daily_revenue desc, order_id

------------------------------

with t as (SELECT DISTINCT creation_time::date as date,
                           sum(price) OVER(PARTITION BY creation_time::date) as daily_revenue
           FROM   (SELECT creation_time,
                          order_id,
                          unnest(product_ids) as product_id
                   FROM   orders) t1 join products using(product_id)
           WHERE  order_id not in (SELECT order_id
                                   FROM   user_actions
                                   WHERE  action like '%cancel%'))
SELECT date,
       daily_revenue,
       coalesce(daily_revenue - lag(daily_revenue, 1) OVER(ORDER BY date),
                0) as revenue_growth_abs,
       coalesce(round(daily_revenue*100/lag(daily_revenue, 1) OVER(ORDER BY date) - 100, 1),
                0) as revenue_growth_percentage
FROM   t

------------------------------

WITH main_table AS 
(SELECT order_price, ROW_NUMBER() OVER (ORDER BY order_price) AS row_number, 
COUNT(*) OVER() AS total_rows 
FROM (SELECT SUM(price) AS order_price 
FROM (SELECT order_id, product_ids, UNNEST(product_ids) AS product_id 
FROM orders 
WHERE order_id NOT IN 
(SELECT order_id FROM user_actions WHERE action='cancel_order') ) t3 
LEFT JOIN products USING(product_id) 
GROUP BY order_id ) t1 ) 
SELECT AVG(order_price) AS median_price FROM main_table 
WHERE row_number BETWEEN total_rows/2.0 AND total_rows/2.0 + 1
