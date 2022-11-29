select 
    t1.*,
    case 
        when t1.currency = 'CADJPY' then t1.rate =  (select CADJPY from buyma_ruoyu.latest_cadjpy ) 
    else 103 
    end as rate
from buyma_ruoyu.exchange_rates as t1