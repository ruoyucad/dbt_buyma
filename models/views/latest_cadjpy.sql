select 
    t1.*,
    case 
        when t1.currency = 'CADJPY' then (select CADJPY from buyma_ruoyu.latest_cadjpy ) 
    else t1.currency 
    end as currency
from buyma_ruoyu.exchange_rates as t1