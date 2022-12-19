with companies AS (
    select * from {{ ref('dim_companies') }}
),

fundamentals as (
    select * from {{ ref('fact_fundamentals') }}
),

report_fundamentals as (
    select
        companies.name_latest as company_name,
        fundamentals.indicator_id as indicator,
        fundamentals.year_2010,
        fundamentals.year_2011,
        fundamentals.year_2012,
        fundamentals.year_2013,
        fundamentals.year_2014,
        fundamentals.year_2015,
        fundamentals.year_2016
                
    from fundamentals
    left join companies using (company_id)
)

select * from report_fundamentals