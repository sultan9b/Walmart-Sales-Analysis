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
	