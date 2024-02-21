SELECT DISTINCT E.PARENT_ENTITY_NAME, SO.pms_application, MIN(SO.MOST_RECENT_SYNC_TIMESTAMP_UTC) as initial_synced_date, count(distinct pv.provider_id) as active_providers_on_sync
FROM sync.sync_overview_vw SO 
     inner join provider.provider_location_mapping PLM on PLM.location_id = SO.location_id AND PLM.IS_ACTIVE AND PLM.approval_status = 'Approved'
     inner join provider.provider PV on PV.provider_id = PLM.provider_id AND NOT PV.is_test and PV.latest_activation_end_timestamp_utc IS NULL AND PV.latest_activation_start_timestamp_utc IS NOT NULL 
    inner join provider_analytics.practice_parent_entity_mapping_vw E on E.practice_id = Pv.practice_id
WHERE pms_application IN ('CernerMillenniumAPI','CernerMillenniumR4')
GROUP BY 1,2


