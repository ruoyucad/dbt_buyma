UPDATE  buyma_ruoyu.exchange_rates

SET rate = (select CADJPY from buyma_ruoyu.exchange_rates)

WHERE currency = 'CADJPY'