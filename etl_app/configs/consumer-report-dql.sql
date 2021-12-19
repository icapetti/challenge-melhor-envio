SELECT authenticated_entity_consumer_id_uuid AS "Consumidor",
       Count(*)                              AS "Quantidade Requisições"
FROM   logs
GROUP  BY authenticated_entity_consumer_id_uuid;