{{
    config(
        materialized='table'
    )
}}


with

attachment as (
    
    select * from {{ ref('attachment') }}

),


final as (
    SELECT 
        call_id,
        timestamp, 
        campaign, 
        call_type, 
        disposition, 
        CASE 
            WHEN disposition NOT LIKE '%AS%' AND disposition NOT LIKE '%BIL%' AND disposition NOT LIKE '%CSR%' AND disposition NOT LIKE '%CS%' 
                AND disposition NOT LIKE '%CAN%'  AND disposition NOT LIKE '%DLR%'  AND disposition NOT LIKE '%Customer Service%'
                AND disposition NOT LIKE '%TSC%' 
            THEN disposition
            ELSE NULL 
        END AS total_calls_disposition,
        
        CASE 
            WHEN disposition LIKE '%SAL%' OR disposition LIKE '%SALE%' 
                OR disposition LIKE '%Sale%' OR disposition LIKE '%sale%' 
            THEN disposition
            ELSE NULL 
        END AS sales_calls_disposition,
        
        CASE 
            WHEN disposition LIKE '%CLOSED%' OR disposition LIKE '%Closed%' 
                OR disposition LIKE '%CLOSE%' 
                OR disposition LIKE '%close%' 
            THEN disposition
            ELSE NULL 
        END AS closed_sales_calls_disposition
    FROM attachment
    WHERE call_type = "Inbound" OR call_type = "Manual"
)

select * from final