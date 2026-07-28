{% snapshot snapshot_order_status %}

{{
    config(
        target_schema='snapshots',
        unique_key='order_id',
        strategy='check',
        check_cols=['order_status', 'order_delivered_customer_date'],
    )
}}

select
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date

from {{ source('olist_raw', 'orders') }}

{% endsnapshot %}