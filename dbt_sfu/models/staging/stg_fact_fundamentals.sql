select * from {{ source('raw_tables', 'raw-indicators_by_company') }}
