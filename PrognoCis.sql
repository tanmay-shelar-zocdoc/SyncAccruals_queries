SET MonthToReport = CAST('2023-05-01' as DATE); -- Set To the first of month we're reporting for i.e. Put 2021-07-01 to send "July's report" to Nextech
SET TotalDaysInMonth = (SELECT LAST_DAY($MonthToReport));

With LastPMS AS -- gives sync IDs with PMS
  (SELECT DISTINCT
     TRY_CAST(S.entity_id as BIGINT) as SyncDBID,
     PMS.pms_application,
     PMS.pms_id
   FROM
     sync.setting S
     INNER JOIN sync.practice_management_software PMS
       on PMS.pms_id = TRY_CAST(S.setting_value as BIGINT) and S.setting_key_id = 18
     INNER JOIN (SELECT
                   setting.entity_id,
                   MAX(event_timestamp_utc) as most_recent_created_or_updated_event
                 FROM sync.setting
                 WHERE setting_key_id = 18 and event_type IN ('Added', 'Updated')
                 GROUP BY 1
                ) MostRecent on MostRecent.entity_id = S.entity_id and
                                MostRecent.most_recent_created_or_updated_event = S.event_timestamp_utc
  ),

SyncLocationMappingHistory AS (
    SELECT
      TRY_CAST(CreatedMappings.SyncDBID as BIGINT) as SyncDBID
      ,CreatedMappings.LocationID
      ,MIN(CreatedMappings.created_date_utc) as created_date_utc 
      ,MAX(DeletedMappings.deleted_date_utc) as deleted_date_utc
    FROM
      (
        SELECT
          TRY_CAST(setting_value as BIGINT)        as SyncDBID,
          entity_id                            as LocationID,
          event_timestamp_utc                  as created_date_utc,
          ROW_NUMBER()
          OVER (
            PARTITION BY TRY_CAST(setting_value as BIGINT), entity_id
            ORDER BY event_timestamp_utc ASC) as CreatedOrder
        FROM sync.setting
        WHERE
          setting_key_id = 16
          AND event_type in ('Added','Updated')
        ) CreatedMappings
      LEFT JOIN
      (
        SELECT
          event_timestamp_utc                  as deleted_date_utc,
          TRY_CAST(setting_value as BIGINT)        as SyncDBID,
          entity_id                            as LocationID,
          ROW_NUMBER()
          OVER (
            PARTITION BY TRY_CAST(setting_value as BIGINT), entity_id
            ORDER BY event_timestamp_utc ASC) as DeleteOrder
        FROM
          sync.setting
        WHERE
          setting.event_type = 'Deleted'
          AND setting_key_id = 16
      ) DeletedMappings on TRY_CAST(DeletedMappings.SyncDBID as BIGINT) = TRY_CAST(CreatedMappings.SyncDBID as BIGINT) and DeletedMappings.LocationID = CreatedMappings.LocationID and DeletedMappings.DeleteOrder = CreatedMappings.CreatedOrder
    GROUP BY 1,2
  ),

PrognoCISPractices AS(
SELECT DISTINCT PT.practice_id, 
             PT.monolith_provider_id    as monolith_practice_id,
             PT.name as practice_name,
             LastPMS.pms_application,
             count(distinct PV.provider_id) as num_active_providers,
             MIN(SLMH.created_date_utc) as initial_synced_date,
             count(distinct StillSyncedToAllscripts.LocationID) as num_locations_still_synced,
             CASE WHEN num_locations_still_synced = 0 THEN MAX(SLMH.deleted_date_utc) ELSE NULL END AS desync_date
FROM LastPMS
      inner join SyncLocationMappingHistory SLMH on SLMH.SyncDBID = LastPMS.SyncDBID
      inner join provider.location L on L.monolith_provider_location_id = SLMH.LocationID
      inner join provider.practice PT on PT.practice_id = L.practice_id and not PT.is_test
      left join provider.provider PV on PV.practice_id = PT.practice_id and PV.latest_activation_start_timestamp_utc IS NOT NULL AND PV.latest_activation_end_timestamp_utc IS NULL
      left join (
          SELECT DISTINCT SLMH.LocationID
          FROM SyncLocationMappingHistory SLMH
               INNER JOIN LastPMS LP on LP.SyncDBID = SLMH.SyncDBID and LP.pms_application = 'CAPI_PrognoCIS'
          WHERE deleted_date_utc IS NULL
    ) StillSyncedToAllscripts on StillSyncedToAllscripts.LocationID = L.monolith_provider_location_id
WHERE LastPMS.pms_application IN ('CAPI_PrognoCIS')
GROUP BY 1,2,3,4
ORDER BY initial_synced_date DESC)

SELECT * 
FROM PrognoCISPractices

