SELECT service_name AS "Serviço",
       Count(*)     AS "Quantidade Requisições",
       Avg(latencies_request) AS "Tempo médio Request",
       Avg(latencies_proxy)   AS "Tempo médio Proxy",
       Avg(latencies_kong)    AS "Tempo médio Kong"
FROM   logs
GROUP  BY service_name;