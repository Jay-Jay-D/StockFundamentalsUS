select * from {{ source('raw_tables', 'raw-companies') }}
