{% snapshot shipments_snapshot%}

{{
    config
            (
                target_database='QWT_ANALYTICS_DEV'
                , target_schema='Snapshots_dev'
                , unique_key="orderid||'-'||lineno"
                , strategy='timestamp'
                , updated_at='SHIPMENTDATE'
            )
}}

select * from {{ref('stg_shipments')}}

{% endsnapshot %}