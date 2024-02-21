SET MonthToReport = CAST('2023-05-01' as DATE); -- Set To the first of month we're reporting for i.e. Put 2021-07-01 to send "July's report" to Dentrix 
SET TotalDaysInMonth = (SELECT DAY(LAST_DAY($MonthToReport)));

WITH SyncLocationMappingHistory AS (
    SELECT
      CreatedMappings.SyncDBID
      ,CreatedMappings.LocationID
      ,CreatedMappings.created_date_utc
      ,DeletedMappings.deleted_date_utc
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
          AND event_type = 'Added'
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
      ) DeletedMappings on DeletedMappings.SyncDBID = CreatedMappings.SyncDBID and DeletedMappings.LocationID = CreatedMappings.LocationID and DeletedMappings.DeleteOrder = CreatedMappings.CreatedOrder
  ),
  
MostRecentPMS AS
  (SELECT DISTINCT
     TRY_CAST(S.entity_id as BIGINT) as SyncDBID,
     PMS.pms_application,
     PMS.pms_id
   FROM
     sync.setting S
     INNER JOIN sync.practice_management_software PMS
       on PMS.pms_id = TRY_CAST(S.setting_value as INT) and S.setting_key_id = 18
     INNER JOIN (SELECT
                   setting.entity_id,
                   MAX(event_timestamp_utc) as most_recent_created_or_updated_event
                 FROM sync.setting
                 WHERE setting_key_id = 18 and event_type IN ('Added', 'Updated')
                 GROUP BY 1
                ) MostRecent on MostRecent.entity_id = S.entity_id and
                                MostRecent.most_recent_created_or_updated_event = S.event_timestamp_utc
  ),

ActiveDenticonSyncsInRange AS 
(SELECT DISTINCT 
    SLMH.SyncDBID,
    SLMH.LocationID,
    L.LOCATION_ID as cloud_location_id,
    PT.practice_id as cloud_practice_id,
    PT.monolith_provider_id as monolith_practice_id,
    SLMH.created_date_utc,
    SLMH.deleted_date_utc
FROM 
  SyncLocationMappingHistory SLMH 
  INNER JOIN provider.location L on L.MONOLITH_PROVIDER_LOCATION_ID = SLMH.LocationID 
  INNER JOIN provider.practice PT on PT.practice_id = L.practice_id
  INNER JOIN MostRecentPMS P on P.SyncDBID = SLMH.SyncDBID and P.pms_application = 'Denticon'
WHERE SLMH.created_date_utc < ADD_MONTHS($MonthToReport,1) AND (SLMH.deleted_date_utc IS NULL OR SLMH.deleted_date_utc > ADD_MONTHS($MonthToReport,1))
 ),
 
ActivePracticesInRange AS 
 (SELECT DISTINCT
    PT.practice_id, PT.name, PT.monolith_provider_id
 FROM provider_analytics.practice_snapshot_summary_day PSM 
      INNER JOIN provider.practice PT on PT.PRACTICE_ID = PSM.practice_id
 WHERE PSM.SNAPSHOT_DATE_EASTERN = $MonthToReport),
 
 
DeletedAggLocationMappings as
 (SELECT aggregate_location_id, source_location_id, deleted_timestamp_utc
  FROM provider_raw.aggregate_location_mapping_update
  WHERE event_update_type IN ('update', 'PROD_manual_backfill_20201119_update')
 ),

SyncedSourceLocations as
    (
        SELECT U.aggregate_location_id, U.source_location_id, created_timestamp_utc
        FROM provider_raw.aggregate_location_mapping_update U
                 LEFT JOIN DeletedAggLocationMappings DM on DM.aggregate_location_id = U.aggregate_location_id and
                                                 DM.source_location_id = U.source_location_id and
                                                 DM.deleted_timestamp_utc > U.created_timestamp_utc
                 INNER JOIN ActiveDenticonSyncsInRange DSY on DSY.cloud_location_id = U.source_location_id
        WHERE event_update_type in ('create','PROD_manual_backfill_20201119_create')
          AND DM.aggregate_location_id IS NULL
        ORDER BY created_timestamp_utc DESC
    ),
  
ActiveProvLocsInRange AS 
  (SELECT provider_id, location_id
   FROM provider_analytics.provider_location_snapshot_summary_month PLSM
   WHERE PLSM.SNAPSHOT_MONTH_EASTERN = add_months($MonthToReport,1) 
   AND PLSM.IS_ACTIVE_BEGINNING_OF_MONTH
   
   UNION 

   SELECT provider_id, SSL.source_location_id as location_id 
   FROM provider_analytics.provider_location_snapshot_summary_month PLSM2
        INNER JOIN SyncedSourceLocations SSL on SSL.aggregate_location_id = PLSM2.location_id
   WHERE PLSM2.SNAPSHOT_MONTH_EASTERN = add_months($MonthToReport,1) 
   AND PLSM2.IS_ACTIVE_BEGINNING_OF_MONTH
  ), 

PrimaryLocation AS 
   (SELECT 
    cloud_practice_ID,
    MIN(locationID) as PrimaryLocationID
   FROM 
    ActiveDenticonSyncsInRange
   GROUP BY 1)
   
SELECT DISTINCT
    AP.practice_id,
    AP.monolith_provider_id as monolith_practice_id,
    AP.name, 
    PL.address1 as Address,
    PL.city,
    PL.state,
    PL.zip_code,
    Numbers.telephone_number,
    Numbers.fax_number, 
    MIN(DS.created_date_utc) as synced_date,
    MAX(total_num_of_providers) as num_of_providers
FROM 
   ActivePracticesInRange AP
   INNER JOIN ActiveDenticonSyncsInRange DS on DS.cloud_practice_id = AP.practice_id
   INNER JOIN Provider.LOCATION PL on PL.monolith_provider_location_id = DS.locationid AND NOT PL.is_virtual
   LEFT JOIN 
    (SELECT location_id, COUNT(DISTINCT provider_id) as total_num_of_providers
     FROM ActiveProvLocsInRange 
     GROUP BY 1) APL on APL.location_id = PL.LOCATION_ID
   LEFT JOIN 
    (SELECT PL.cloud_practice_id, 
        L.phone as telephone_number,
        L.fax as fax_number
    FROM PrimaryLocation PL
    INNER JOIN provider.location L on L.monolith_provider_location_id = PL.PrimaryLocationID) Numbers on Numbers.cloud_practice_id = AP.practice_id 
WHERE 
    APL.total_num_of_providers IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8,9;


—--------------------------------------------------------------------------------

SET MonthToReport = CAST('2024-01-01' as DATE); -- Set To the first of month we're reporting for i.e. Put 2021-07-01 to send "July's report" to Dentrix 
SET TotalDaysInMonth = (SELECT DAY(LAST_DAY($MonthToReport)));

WITH SyncLocationMappingHistory AS (
    SELECT
      CreatedMappings.SyncDBID
      ,CreatedMappings.LocationID
      ,CreatedMappings.created_date_utcCn 
      ,DeletedMappings.deleted_date_utc
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
          AND event_type = 'Added'
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
      ) DeletedMappings on DeletedMappings.SyncDBID = CreatedMappings.SyncDBID and DeletedMappings.LocationID = CreatedMappings.LocationID and DeletedMappings.DeleteOrder = CreatedMappings.CreatedOrder
  ),
  
MostRecentPMS AS
  (SELECT DISTINCT
     TRY_CAST(S.entity_id as BIGINT) as SyncDBID,
     PMS.pms_application,
     PMS.pms_id
   FROM
     sync.setting S
     INNER JOIN sync.practice_management_software PMS
       on PMS.pms_id = TRY_CAST(S.setting_value as INT) and S.setting_key_id = 18
     INNER JOIN (SELECT
                   setting.entity_id,
                   MAX(event_timestamp_utc) as most_recent_created_or_updated_event
                 FROM sync.setting
                 WHERE setting_key_id = 18 and event_type IN ('Added', 'Updated')
                 GROUP BY 1
                ) MostRecent on MostRecent.entity_id = S.entity_id and
                                MostRecent.most_recent_created_or_updated_event = S.event_timestamp_utc
  ),

ActiveDenticonSyncsInRange AS 
(SELECT DISTINCT 
    SLMH.SyncDBID,
    SLMH.LocationID,
    L.LOCATION_ID as cloud_location_id,
    PT.practice_id as cloud_practice_id,
    PT.monolith_provider_id as monolith_practice_id,
    SLMH.created_date_utc,
    SLMH.deleted_date_utc
FROM 
  SyncLocationMappingHistory SLMH 
  INNER JOIN provider.location L on L.MONOLITH_PROVIDER_LOCATION_ID = SLMH.LocationID 
  INNER JOIN provider.practice PT on PT.practice_id = L.practice_id
  INNER JOIN MostRecentPMS P on P.SyncDBID = SLMH.SyncDBID and P.pms_application = 'Denticon'
WHERE SLMH.created_date_utc < ADD_MONTHS($MonthToReport,1) AND (SLMH.deleted_date_utc IS NULL OR SLMH.deleted_date_utc > ADD_MONTHS($MonthToReport,1))
 ),
 
ActivePracticesInRange AS 
 (SELECT DISTINCT
    PT.practice_id, PT.name, PT.monolith_provider_id
 FROM provider_analytics.practice_snapshot_summary_day PSM 
      INNER JOIN provider.practice PT on PT.PRACTICE_ID = PSM.practice_id
 WHERE PSM.SNAPSHOT_DATE_EASTERN = $MonthToReport),
 
 
DeletedAggLocationMappings as
 (SELECT aggregate_location_id, source_location_id, deleted_timestamp_utc
  FROM provider_raw.aggregate_location_mapping_update
  WHERE event_update_type IN ('update', 'PROD_manual_backfill_20201119_update')
 ),

SyncedSourceLocations as
    (
        SELECT U.aggregate_location_id, U.source_location_id, created_timestamp_utc
        FROM provider_raw.aggregate_location_mapping_update U
                 LEFT JOIN DeletedAggLocationMappings DM on DM.aggregate_location_id = U.aggregate_location_id and
                                                 DM.source_location_id = U.source_location_id and
                                                 DM.deleted_timestamp_utc > U.created_timestamp_utc
                 INNER JOIN ActiveDenticonSyncsInRange DSY on DSY.cloud_location_id = U.source_location_id
        WHERE event_update_type in ('create','PROD_manual_backfill_20201119_create')
          AND DM.aggregate_location_id IS NULL
        ORDER BY created_timestamp_utc DESC
    ),
  
ActiveProvLocsInRange AS 
  (SELECT provider_id, location_id
   FROM provider_analytics.provider_location_snapshot_summary_month PLSM
   WHERE PLSM.SNAPSHOT_MONTH_EASTERN = add_months($MonthToReport,1) 
   AND PLSM.IS_ACTIVE_BEGINNING_OF_MONTH
   
   UNION 

   SELECT provider_id, SSL.source_location_id as location_id 
   FROM provider_analytics.provider_location_snapshot_summary_month PLSM2
        INNER JOIN SyncedSourceLocations SSL on SSL.aggregate_location_id = PLSM2.location_id
   WHERE PLSM2.SNAPSHOT_MONTH_EASTERN = add_months($MonthToReport,1) 
   AND PLSM2.IS_ACTIVE_BEGINNING_OF_MONTH
  ), 

PrimaryLocation AS 
   (SELECT 
    cloud_practice_ID,
    MIN(locationID) as PrimaryLocationID
   FROM 
    ActiveDenticonSyncsInRange
   GROUP BY 1)
   
SELECT DISTINCT
    AP.practice_id,
    AP.monolith_provider_id as monolith_practice_id,
    AP.name, 
    PL.address1 as Address,
    PL.city,
    PL.state,
    PL.zip_code,
    Numbers.telephone_number,
    Numbers.fax_number, 
    MIN(DS.created_date_utc) as synced_date,
    MAX(total_num_of_providers) as num_of_providers
FROM 
   ActivePracticesInRange AP
   INNER JOIN ActiveDenticonSyncsInRange DS on DS.cloud_practice_id = AP.practice_id
   INNER JOIN Provider.LOCATION PL on PL.monolith_provider_location_id = DS.locationid AND NOT PL.is_virtual
   LEFT JOIN 
    (SELECT location_id, COUNT(DISTINCT provider_id) as total_num_of_providers
     FROM ActiveProvLocsInRange 
     GROUP BY 1) APL on APL.location_id = PL.LOCATION_ID
   LEFT JOIN 
    (SELECT PL.cloud_practice_id, 
        L.phone as telephone_number,
        L.fax as fax_number
    FROM PrimaryLocation PL
    INNER JOIN provider.location L on L.monolith_provider_location_id = PL.PrimaryLocationID) Numbers on Numbers.cloud_practice_id = AP.practice_id 
WHERE 
    APL.total_num_of_providers IS NOT NULL 
GROUP BY 1,2,3,4,5,6,7,8,9;

