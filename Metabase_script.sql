/* Вкладка "Продажи" */

-- Чистый доход и количество заказов за все время
select
    date_trunc('month', order_purchase_timestamp) as "месяц-год",
    sum(price) "Чистый доход",
    sum(payment_value) "Денежный оборот",
    count(distinct order_id) "Заказы"
from dm.sales
group by 1
order by 1;

-- Значение метрики
with max_date as (
    select max(order_purchase_timestamp) as last_date from dm.sales
    )
select 
    case
        when {{metric}} = 'Денежный оборот' then sum(payment_value)
        when {{metric}} = 'Чистый доход' then sum(price)
        when {{metric}} = 'Кол-во заказов' then count(distinct order_id)
        when {{metric}} = 'Кол-во клиентов' then count(distinct customer_id)
        when {{metric}} = 'Средний чек (AOV)' then sum(payment_value) / count(distinct order_id)
        when {{metric}} = 'Средний доход (ARPU)' then sum(payment_value) / count(distinct customer_id)
    end as Значение
from dm.sales s, max_date m
where order_status not in ('canceled', 'unavailable')
 and s.order_purchase_timestamp >= 
        case 
            when {{period}} = 'За последний месяц' then m.last_date - interval '1 month'
            when {{period}} = 'За последний квартал' then m.last_date - interval '3 month'
            when {{period}} = 'За последний год' then m.last_date - interval '1 year'
        end;

-- В динамике
with max_date as (
    select max(order_purchase_timestamp) as last_date from dm.sales
    )
select 
    case
        when {{period}} = 'За последний год' then date_trunc('month', order_purchase_timestamp)
        else date_trunc('week', order_purchase_timestamp)
    end as period_date,
    case
        when {{metric}} = 'Денежный оборот' then sum(payment_value)
        when {{metric}} = 'Чистый доход' then sum(price)
        when {{metric}} = 'Кол-во заказов' then count(distinct order_id)
        when {{metric}} = 'Кол-во клиентов' then count(distinct customer_id)
        when {{metric}} = 'Средний чек (AOV)' then sum(payment_value) / count(distinct order_id)
        when {{metric}} = 'Средний доход (ARPU)' then sum(payment_value) / count(distinct customer_id)
    end as Значение
from dm.sales s, max_date m
where s.order_status not in ('canceled', 'unavailable')
    and s.order_purchase_timestamp >= 
        case 
            when {{period}} = 'За последний месяц' then m.last_date - interval '1 month'
            when {{period}} = 'За последний квартал' then m.last_date - interval '3 month'
            when {{period}} = 'За последний год' then m.last_date - interval '1 year'
        end
group by 1
order by 1;

-- Доля по штатам
with max_date as (
    select max(order_purchase_timestamp) as last_date from dm.sales
    )
select customer_state as Штат,
    case
        when {{metric}} = 'Денежный оборот' then sum(payment_value)
        when {{metric}} = 'Чистый доход' then sum(price)
        when {{metric}} = 'Кол-во заказов' then count(distinct order_id)
        when {{metric}} = 'Кол-во клиентов' then count(distinct customer_id)
        when {{metric}} = 'Средний чек (AOV)' then sum(payment_value) / count(distinct order_id)
        when {{metric}} = 'Средний доход (ARPU)' then sum(payment_value) / count(distinct customer_id)
    end as Значение
from dm.sales s, max_date m
where order_status not in ('canceled', 'unavailable')
    and s.order_purchase_timestamp >= 
        case 
            when {{period}} = 'За последний месяц' then m.last_date - interval '1 month'
            when {{period}} = 'За последний квартал' then m.last_date - interval '3 month'
            when {{period}} = 'За последний год' then m.last_date - interval '1 year'
        end
group by 1
order by 1;

-- Топ-10 категории товаров по метрике
with max_date as (
    select max(order_purchase_timestamp) as last_date from dm.sales
    )
select product_category as "Категория товаров", 
    case
        when {{metric}} = 'Денежный оборот' then sum(payment_value)
        when {{metric}} = 'Чистый доход' then sum(price)
        when {{metric}} = 'Кол-во заказов' then count(distinct order_id)
        when {{metric}} = 'Кол-во клиентов' then count(distinct customer_id)
        when {{metric}} = 'Средний чек (AOV)' then sum(payment_value) / count(distinct order_id)
        when {{metric}} = 'Средний доход (ARPU)' then sum(payment_value) / count(distinct customer_id)
    end as Значение
from dm.sales s, max_date m
where s.order_status not in ('canceled', 'unavailable')
    and s.order_purchase_timestamp >= 
        case 
            when {{period}} = 'За последний месяц' then m.last_date - interval '1 month'
            when {{period}} = 'За последний квартал' then m.last_date - interval '3 month'
            when {{period}} = 'За последний год' then m.last_date - interval '1 year'
        end
group by 1
order by 2 desc
limit 10;

-- Показатели в разрезе методов оплаты заказов
select payment_type,
    sum(payment_value) "Денежные обороты",
    sum(price) as "Чистый доход",
    sum(payment_value)/count(distinct order_id) "Средний чек (AOV)",
    sum(payment_value)/count(distinct customer_id) "Средний доход (ARPU)",
    count(distinct order_id) "Кол-во заказов",
    count(distinct customer_id) "Кол-во клиентов"
from dm.sales
where order_status not in ('canceled', 'unavailable')
group by payment_type
order by 2 desc;



/* Вкладка "Логистика"*/

-- % заказов > 14 дней
select round(100 * count(distinct order_id) filter (where delivery_days > 14)/count(distinct order_id), 3)/100 from dm.delivery;

-- Динамика времени доставки
select date_trunc('month', o.order_delivered_customer_date) as "Месяц-Год",
    avg(extract(day from (o.order_delivered_customer_date - o.order_delivered_carrier_date))) as "Среднее время доставки"
    /*avg(extract(day from (o.order_delivered_customer_date - o.order_delivered_carrier_date))) - 
    avg(extract(day from (o.order_estimated_delivery_date - o.order_delivered_customer_date))) as "df"*/
from raw.orders o
where o.order_delivered_customer_date is not null
		  and o.order_delivered_carrier_date is not null
group by 1;

-- Среднее время доставки по штатам
select customer_state "Штат",
    avg(delivery_days) "Средняя время"
from dm.delivery
group by 1
order by 2 desc;

-- Распределение дней доставки
select 
    case
        when delivery_days < 0 then 'Меньше дня'
        when delivery_days < 3 then '0-3 дня'
        when delivery_days < 7 then '4-7 дней'
        when delivery_days < 14 then '8-14 дней'
        when delivery_days < 30 then '14-30 дней'
        when delivery_days < 60 then '1-2 месяца'
        when delivery_days < 90 then '2-3 месяца'
    else 'Больше 3 месяцев'
    end as "Дни доставки", 
    count(distinct order_id) as "Кол-во доставок"
from dm.delivery
group by 1
order by min(delivery_days);

-- Проблемные маршруты со средним временем доставки больше трех месяцев
select seller_city || ' -> ' || customer_city as "Проблемный маршрут",
    seller_state "Штат продавца",
    customer_state "Штат покупателя",
    avg(delivery_days) as "Среднее время доставки в днях",
    count(distinct order_id) as "Кол-во заказов"
from dm.delivery
group by 1, 2, 3
having  avg(delivery_days) > 90
order by 4 desc;

-- Топ 10 городов по скорости доставки
select seller_city "Город",
    avg(delivery_days) "Среднее время доставки",
    count(distinct order_id) "Кол-во доставок"
from dm.delivery
group by 1
order by 2
limit 10;


/*Вкладка "ABC/XYZ"*/
-- ABC анализ товаров
with pareto as (
select product_category, sum(price) as category_revenue,
		sum(sum(price)) over() as total_revenue,
		round(100 * sum(price) / sum(sum(price)) over(), 3) as pct_total,
		round(100 * sum(sum(price))	over(order by sum(price) desc) / sum(sum(price)) over (), 3) as cumulative_pct,
		sum(payment_value) cash_flow,
		count(distinct order_id) order_count,
		sum(price)/count(distinct order_id) as AOV
from dm.sales
where order_status not in ('canceled', 'unavailable')
group by 1	
order by 4 desc
),
abc_analysis as (
select *,
	case
		when cumulative_pct <= 80 then 'A'
		when cumulative_pct <= 95 then 'B'
		else 'C' end as abc_group
from pareto
)
select abc_group as "Группа", 
    count(product_category) || ' (' || round(100 * count(product_category)/sum(count(product_category)) over (), 2) || '%)' as "Кол-во категории",
    sum(category_revenue) || ' (' || round(100 * sum(category_revenue) / sum(sum(category_revenue)) over(), 2) || '%)' as "Прибыль",
    sum(cash_flow) as "Денежный оборот",
    sum(order_count) as "Кол-во заказов",
    avg(AOV) as "Средний чек (AOV)"
from abc_analysis
group by 1
order by 3;


-- Доля групп по прибыли
with pareto as (
select product_category, sum(price) as category_revenue,
		sum(sum(price)) over() as total_revenue,
		round(100 * sum(price) / sum(sum(price)) over(), 3) as pct_total,
		round(100 * sum(sum(price))	over(order by sum(price) desc) / sum(sum(price)) over (), 3) as cumulative_pct,
		sum(payment_value) cash_flow,
		count(distinct order_id) order_count,
		sum(price)/count(distinct order_id) as AOV
from dm.sales
where order_status not in ('canceled', 'unavailable')
group by 1	
order by 4 desc
),
abc_analysis as (
select *,
	case
		when cumulative_pct <= 80 then 'A'
		when cumulative_pct <= 95 then 'B'
		else 'C' end as abc_group
from pareto
)
select abc_group as "Группа",
    sum(category_revenue) as "Прибыль"
from abc_analysis
group by 1;

-- ABC анализ штатов
with pareto as (
select customer_state, sum(price) as category_revenue,
		sum(sum(price)) over() as total_revenue,
		round(100 * sum(price) / sum(sum(price)) over(), 3) as pct_total,
		round(100 * sum(sum(price))	over(order by sum(price) desc) / sum(sum(price)) over (), 3) as cumulative_pct,
		sum(payment_value) cash_flow,
		count(distinct order_id) order_count,
		sum(price)/count(distinct order_id) as AOV
from dm.sales
where order_status not in ('canceled', 'unavailable')
group by 1	
order by 4 desc
),
abc_analysis as (
select *,
	case
		when cumulative_pct <= 80 then 'A'
		when cumulative_pct <= 95 then 'B'
		else 'C' end as abc_group
from pareto
)
select abc_group as "Группа",
    count(customer_state) || ' (' || round(100 * count(customer_state)/sum(count(customer_state)) over (), 2) || '%)' as "Кол-во штатов",
    sum(category_revenue) || ' (' || round(100 * sum(category_revenue) / sum(sum(category_revenue)) over(), 2) || '%)' as "Прибыль",
    sum(cash_flow) as "Денежный оборот",
    sum(order_count) as "Кол-во заказов",
    avg(AOV) as "Средний чек (AOV)"
from abc_analysis
group by 1
order by 1, 4 desc;

-- Подробнее по каждому штату
with pareto as (
select customer_state, sum(price) as category_revenue,
		sum(sum(price)) over() as total_revenue,
		round(100 * sum(price) / sum(sum(price)) over(), 3) as pct_total,
		round(100 * sum(sum(price))	over(order by sum(price) desc) / sum(sum(price)) over (), 3) as cumulative_pct,
		sum(payment_value) cash_flow,
		count(distinct order_id) order_count,
		sum(price)/count(distinct order_id) as AOV
from dm.sales
where order_status not in ('canceled', 'unavailable')
group by 1	
order by 4 desc
),
abc_analysis as (
select *,
	case
		when cumulative_pct <= 80 then 'A'
		when cumulative_pct <= 95 then 'B'
		else 'C' end as abc_group
from pareto
)
select abc_group as "Группа",
    customer_state as "Штат",
    sum(category_revenue) || ' (' || round(100 * sum(category_revenue) / sum(sum(category_revenue)) over(), 2) || '%)' as "Прибыль",
    sum(cash_flow) as "Денежный оборот",
    sum(order_count) as "Кол-во заказов",
    avg(AOV) as "Средний чек (AOV)"
from abc_analysis
group by 1, 2
order by 1, 4 desc;




