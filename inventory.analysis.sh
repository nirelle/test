#!/usr/bin/env bash

today=$(date +"%Y%m%d")

sql="
insert into table bigdata_bi.o2o_inventory partition(dt = $today)
SELECT distinct ww.matnr as matnr,
        ss.ware_sku_id as sku_id,
        ww.title as ware_name,
        ss.shop_id as shop_id,
        s.store_name as store_name,
        ss.stock as stock,
        from_unixtime(unix_timestamp()) as created,
        store.district_name as district_name,
        ware.first_category as first_category,
        ware.second_category as second_category,
        store.vender_name as vendor_name,
        s.sap_id as sap_id
    from(
    SELECT *
    FROM dmall_ware.sap_location_ware s
    WHERE s.ware_status = 1
    AND s.sap_ware_status=1
    AND s.shop_id not in (63,410,394,10474,432)
    ) ss
    INNER JOIN
    (
        SELECT w.ware_id,w.title,w.rf_id as matnr
        FROM dmall_ware.ware_ware w
        WHERE (
        w.ware_tag & 1 != 1
        AND w.ware_tag & 4 != 4
        AND w.ware_tag & 8 != 8
        AND w.ware_tag & 16 != 16
        AND w.ware_tag & 32 != 32
        AND w.ware_tag & 64 != 64
        AND w.ware_tag & 128 != 128
        )
    ) ww ON ss.ware_id = ww.ware_id
INNER JOIN dmall_oop.store s ON s.id=ss.shop_id and s.yn = 1 and s.flag_online = 1 and s.status = 1 and s.flag_open=1 and s.vender_id = 1
left join business_operation.store_info store ON ss.shop_id = store.store_id
left join business_operation.ware_info ware ON ware.sku_id = ss.ware_sku_id
"
hive -e "$sql"