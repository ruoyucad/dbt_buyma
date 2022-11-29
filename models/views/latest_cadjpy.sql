select 
    t1.*,
    case 
        when t1.currency = 'CADJPY' then (select CADJPY from buyma_ruoyu.exchange_rates ) 
    else t1.currency 
    end as currency
from {{ref('exchange_rates')}} as t1