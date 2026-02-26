-- создание схем
create schema raw;
create schema dm;

-- СОЗДАНИЕ ТАБЛИЦ ДЛЯ ЗАГРУЗКИ ДАННЫХ С MINIO
-- таблица клиентов
create table if not exists raw.customers (
	customer_id				 varchar(32) primary key,
	customer_unique_id 	   	 varchar(32),
	customer_zip_code_prefix int,
	customer_city 			 varchar(50),
	customer_state			 varchar(2)
	);

-- таблица геолокации
create table if not exists raw.geolocation (
	geolocation_zip_code_prefix    int,
	geolocation_lat                decimal(10, 6),
	geolocation_lng                decimal(10, 6),
	geolocation_city               varchar(50),
	geolocation_state              varchar(2)
	);
	
-- таблица продавцов
create table if not exists raw.sellers (
	seller_id                 varchar(32) primary key,
	seller_zip_code_prefix    int,
	seller_city               varchar(50),
	seller_state              varchar(2)
	);
	
-- таблица продуктов
create table if not exists raw.products (
	product_id                    varchar(32) primary key,
	product_category              varchar(50),
	product_name_length           int,
	product_description_length    int,
	product_photos_qty            int,
	product_weight_g              decimal(10, 2),
	product_length_cm             decimal(6, 2),
	product_height_cm             decimal(6, 2),
	product_width_cm              decimal(6, 2)
	);
	
-- таблица заказов
create table if not exists raw.orders (	
	order_id                         varchar(32) primary key,
	customer_id                      varchar(32),
	order_status                     varchar(15),
	order_purchase_timestamp         timestamp,
	order_approved_at                timestamp,
	order_delivered_carrier_date     timestamp,
	order_delivered_customer_date    timestamp,
	order_estimated_delivery_date    timestamp
	);
	
-- таблица продуктов в заказах
create table if not exists raw.order_items (
	order_id                varchar(32),
	order_item_id           int,
	product_id              varchar(32),
	seller_id               varchar(32),
	shipping_limit_date     timestamp,
	price                  	decimal(6, 2),
	freight_value          	decimal(5, 2),
	primary key(order_id, order_item_id)
	);
	
	
-- таблица оплат
create table if not exists raw.payments (
	order_id                 varchar(32),
	payment_sequential       int,
	payment_type             varchar(15),
	payment_installments     int,
	payment_value            decimal(7, 2),
	primary key(order_id, payment_sequential)
	);

	
	
-- ИЗМЕНЕНИЕ ТАБЛИЦ (ДОБАВЛЕНИЕ ВНЕШНИХ СВЯЗЕЙ)
-- orders -> customers 
alter table raw.orders add constraint fk_order_customer
foreign key (customer_id) references raw.customers(customer_id);
	
-- order_items -> orders
alter table raw.order_items add constraint fk_order_items_order
foreign key (order_id) references raw.orders(order_id);	
	
	
-- order_items -> products
alter table raw.order_items add constraint fk_order_items_products
foreign key (product_id) references raw.products(product_id);	
		
-- order_items -> sellers
alter table raw.order_items add constraint fk_order_items_seller
foreign key (seller_id) references raw.sellers(seller_id);		
	
-- payments -> orders
alter table raw.payments add constraint fk_payments_order
foreign key (order_id) references raw.orders(order_id);		


-- Одна главная витрина с уже готовыми JOIN
create table dm.sales as
select 
    o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    p.product_category,
    c.customer_id,
    c.customer_city,
    c.customer_state,
    s.seller_city,
    s.seller_state,
    oi.price,
    oi.freight_value,
    pay.payment_type,
    pay.payment_value
from raw.orders o
join raw.customers c on o.customer_id = c.customer_id
join raw.order_items oi on o.order_id = oi.order_id
join raw.products p on oi.product_id = p.product_id
join raw.sellers s on oi.seller_id = s.seller_id
join raw.payments pay on o.order_id = pay.order_id;

-- Витрина о логистике
create table dm.delivery as 
	select o.order_id,
		   s.seller_id,
		   s.seller_state,
		   s.seller_city,
		   o.order_delivered_carrier_date,
		   c.customer_id,
		   c.customer_state,
		   c.customer_city,
		   o.order_delivered_customer_date,
		   extract(day from (o.order_delivered_customer_date - o.order_delivered_carrier_date)) as delivery_days,
		   extract(hour from (o.order_delivered_customer_date - o.order_delivered_carrier_date)) as delivery_hours,
		   concat(extract(day from (o.order_delivered_customer_date - o.order_delivered_carrier_date)), ' days ',
	    		extract(hour from (o.order_delivered_customer_date - o.order_delivered_carrier_date)), ' hours')
	       as delivery_time
	from raw.customers c
	join raw.orders o using(customer_id)
	join raw.order_items oi using(order_id)
	join raw.sellers s using(seller_id)
	where o.order_delivered_customer_date is not null
		  and o.order_delivered_carrier_date is not null; 

-- Витрина о покупательских способностях
create table dm.purchasing_power as
	select c.customer_id,
		   c.customer_unique_id,
		   c.customer_state,
		   c.customer_city,
		   p.payment_type,
		   p.payment_installments,
		   p.payment_value,
		   o.order_id,
		   o.order_purchase_timestamp
	from raw.customers c
	join raw.orders o using(customer_id)
	join raw.payments p using(order_id)
	where o.order_status = 'delivered';










