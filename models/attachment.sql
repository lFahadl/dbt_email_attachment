with

source as (
    
    select * from {{ source('email_attachment', 'attachment') }}

),  


staged as (

    select
        call_id,
        timestamp, 
        campaign, 
        call_type, 
        disposition 
    from source

)

select * from staged