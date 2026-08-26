CREATE OR REFRESH STREAMING TABLE bronze_sales_orders
AS SELECT 
    order_number,
    customer_id,
    customer_name,
    number_of_line_items,
    order_datetime,
    ordered_products,
    promo_info,
    current_timestamp() as ingestion_timestamp
FROM STREAM(databricks_simulated_retail_customer_data.v01.sales_orders);

CREATE OR REFRESH STREAMING TABLE bronze_customers_cdc
AS SELECT 
    cast(customer_id as int) as customer_id,
    cast(nombre as string) as nombre,
    cast(email as string) as email,
    cast(ciudad as string) as ciudad,
    cast(tier_membresia as string) as tier_membresia,
    cast(timestamp_cambio as timestamp) as timestamp_cambio,
    cast(operacion_cdc as string) as operacion_cdc,
    current_timestamp() as ingestion_timestamp
FROM STREAM read_files(
    '${cdc_volume_path}',
    format => 'json'
);

CREATE OR REFRESH STREAMING TABLE silver_sales_orders (
    CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_line_items EXPECT (number_of_line_items > 0)
)
AS SELECT 
    cast(order_number as bigint) as order_number,
    cast(customer_id as bigint) as customer_id,
    trim(customer_name) as customer_name,
    cast(number_of_line_items as int) as number_of_line_items,
    to_timestamp(from_unixtime(order_datetime)) as order_datetime,
    year(to_timestamp(from_unixtime(order_datetime))) as order_year,
    month(to_timestamp(from_unixtime(order_datetime))) as order_month,
    CASE 
        WHEN number_of_line_items >= 5 THEN 'Pedido Grande'
        WHEN number_of_line_items >= 2 THEN 'Pedido Mediano'
        ELSE 'Pedido Pequeño'
    END as categoria_pedido,
    trim(promo_info) as promo_info,
    ingestion_timestamp
FROM STREAM(LIVE.bronze_sales_orders)
WHERE order_number IS NOT NULL;

CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO LIVE.silver_customers
FROM STREAM(LIVE.bronze_customers_cdc)
KEYS (customer_id)
APPLY AS DELETE WHEN operacion_cdc = 'DELETE'
SEQUENCE BY timestamp_cambio
COLUMNS * EXCEPT (operacion_cdc, ingestion_timestamp)
STORED AS SCD TYPE 2;

CREATE OR REFRESH MATERIALIZED VIEW gold_sales_by_customer_tier
AS SELECT 
    coalesce(c.tier_membresia, 'General') as tier_membresia,
    coalesce(c.ciudad, 'Nacional') as ciudad,
    s.order_year,
    s.categoria_pedido,
    count(distinct s.order_number) as total_ordenes,
    count(distinct s.customer_id) as total_clientes_activos,
    sum(s.number_of_line_items) as total_items_vendidos,
    avg(s.number_of_line_items) as promedio_items_por_orden
FROM LIVE.silver_sales_orders s
LEFT JOIN LIVE.silver_customers c
    ON s.customer_id = c.customer_id
    AND c.__END_AT IS NULL
GROUP BY 
    coalesce(c.tier_membresia, 'General'),
    coalesce(c.ciudad, 'Nacional'),
    s.order_year,
    s.categoria_pedido;

CREATE OR REFRESH MATERIALIZED VIEW metric_view_retail_sales
AS SELECT 
    tier_membresia,
    ciudad,
    order_year,
    categoria_pedido,
    total_ordenes,
    total_clientes_activos,
    total_items_vendidos,
    promedio_items_por_orden,
    round(total_items_vendidos / nullif(total_clientes_activos, 0), 2) as items_promedio_por_cliente
FROM LIVE.gold_sales_by_customer_tier;

