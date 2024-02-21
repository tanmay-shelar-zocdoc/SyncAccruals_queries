With Total_Provider_count_q1 as 
(
select PROVIDER_ID C_Docter_ID,
       MONOLITH_PROFESSIONAL_ID Docter_ID,
       STATUS,
       MONOLITH_PROVIDER_ID D_Practice_ID,
       PRACTICE_ID C_Practice_ID,
  FIRST_NAME,
  LAST_NAME,
  TITLE,
  latest_activation_start_timestamp_utc,
  latest_activation_end_timestamp_utc
from "CISTERN"."PROVIDER"."PROVIDER" 
where 1=1
  and latest_activation_start_timestamp_utc is not null
and latest_activation_end_timestamp_utc is null
  and IS_RESOURCE='FALSE' 
  AND IS_TEST='FALSE' 
  AND PROFILE_TYPE='FullProfile' 
  AND STATUS in ('Approved Application') 
--    And ACTIVATION_TIME_UTC IS Not NULL 
    AND IS_BOOKABLE_LEGACY in('True','TRUE')
  order by LAST_NAME
),

Total_Provider_count_q2 as (
  select Distinct aa.SNAPSHOT_DATE_EASTERN, aa.PROVIDER_ID C_Docter_ID, 
         aa.SYNC_TYPE,
         aa.ACCOUNT_SEGMENT,
         aa.CURRENT_ACCOUNT_SEGMENT,
         aa.BOOKABLE_STATUS,
         aa.PROVIDER_TENURE_MONTHS,
         aa.BOOKABLE_STATUS_BEGINNING_OF_WEEK,
         aa.WAS_EVER_PREVIOUSLY_MP_BOOKABLE,
        ROW_NUMBER() OVER (partition by C_Docter_ID order by SNAPSHOT_DATE_EASTERN desc) as rownum
  from "CISTERN"."VISUALIZATION"."PROVIDER_EXPLORER_DAY_WEEK_VW" aa
  where 1 = 1 qualify rownum = 1
  order by C_Docter_ID, SNAPSHOT_DATE_EASTERN desc
),

acc_segmet_provider as(
select Distinct q1.C_Docter_ID,
       q1.Docter_ID,
       q1.D_Practice_ID,
       q1.C_Practice_ID,
  q1.FIRST_NAME,
  q1.LAST_NAME,
  q1.TITLE,
       q2.CURRENT_ACCOUNT_SEGMENT,
       q2.ACCOUNT_SEGMENT,
       q2.BOOKABLE_STATUS,
       q2.PROVIDER_TENURE_MONTHS,
       q2.BOOKABLE_STATUS_BEGINNING_OF_WEEK,
       q2.WAS_EVER_PREVIOUSLY_MP_BOOKABLE
--   q1.latest_activation_start_timestamp_utc,
--  q1.latest_activation_end_timestamp_utc
        from Total_Provider_count_q1 q1
        LEFT JOIN Total_Provider_count_q2 q2
        ON q1.C_Docter_ID=q2.C_Docter_ID
  ),

Opp_view as(
select 
Distinct SNAPSHOT_DATE_EST,
PRACTICE_ID,
MONOLITH_PRACTICE_ID,
PROVIDER_ID,
MONOLITH_PROFESSIONAL_ID,
MAIN_SPECIALTY,
IS_ACTIVE,
IS_CURRENTLY_SYNCED,
LATEST_ACTIVATION_START_TIMESTAMP_UTC, 
LATEST_ACTIVATION_END_TIMESTAMP_UTC is null,
SYNC_ID,
SYNC_TYPE,
PRACTICE_MANAGEMENT_SYSTEM_LISTED_ON_DSP,
SYNCED_PRACTICE_MANAGEMENT_SYSTEM,
ROW_NUMBER() OVER (partition by PROVIDER_ID order by SNAPSHOT_DATE_EST desc) as rownum
from "CISTERN"."PROVIDER_ANALYTICS"."OPPORTUNITY_INDEX"
where 1 = 1 qualify rownum = 1 
AND IS_ACTIVE in ('TRUE','True')
order by PROVIDER_ID, SNAPSHOT_DATE_EST desc
),

final_list as (
select distinct asp.C_Docter_ID,
       asp.Docter_ID,
       asp.D_Practice_ID,
       asp.C_Practice_ID,
  asp.FIRST_NAME,
  asp.LAST_NAME,
  asp.TITLE,
       asp.CURRENT_ACCOUNT_SEGMENT,
       ov.MAIN_SPECIALTY,
       ov.IS_ACTIVE,
       ov.IS_CURRENTLY_SYNCED,
       ov.SYNC_ID,
       ov.SYNC_TYPE,
       ov.PRACTICE_MANAGEMENT_SYSTEM_LISTED_ON_DSP,
       ov.SYNCED_PRACTICE_MANAGEMENT_SYSTEM,
       asp.BOOKABLE_STATUS,
       asp.PROVIDER_TENURE_MONTHS,
       asp.BOOKABLE_STATUS_BEGINNING_OF_WEEK,
       asp.WAS_EVER_PREVIOUSLY_MP_BOOKABLE
       from acc_segmet_provider asp
       left join Opp_view ov
       on asp.C_Docter_ID=ov.PROVIDER_ID
),

SF_list as (
  select PROVIDER_ID__C Practice_ID,
    PROFESSIONAL_ID__C,
   // NAME,
    FIRSTNAME,
    LASTNAME,
    EMAIL,
    //VERIFIED_EMAIL__C,
    PHONE,
    MOBILEPHONE
   // CONTACT_WITH_PROVIDER_ID__C,
  //  MARKET__C
    from "CISTERN"."SALESFORCE"."CONTACT_VW" sf_contact
),

final_list_SF as (
select distinct fl.C_Docter_ID,
       fl.Docter_ID,
       fl.D_Practice_ID,
       fl.C_Practice_ID,
       fl.FIRST_NAME,
       fl.LAST_NAME,
       fl.TITLE,
       sl.EMAIL,
      // sl.VERIFIED_EMAIL__C,
       sl.PHONE,
       sl.MOBILEPHONE,
    //   sl.CONTACT_WITH_PROVIDER_ID__C,
       fl.CURRENT_ACCOUNT_SEGMENT,
       fl.MAIN_SPECIALTY,
       fl.IS_ACTIVE,
       fl.IS_CURRENTLY_SYNCED,
       fl.SYNC_ID,
       fl.SYNC_TYPE,
       fl.PRACTICE_MANAGEMENT_SYSTEM_LISTED_ON_DSP,
       fl.SYNCED_PRACTICE_MANAGEMENT_SYSTEM,
       fl.BOOKABLE_STATUS,
       fl.PROVIDER_TENURE_MONTHS,
       fl.BOOKABLE_STATUS_BEGINNING_OF_WEEK,
       fl.WAS_EVER_PREVIOUSLY_MP_BOOKABLE
    //   sl.MARKET__C
       from final_list fl
       left join SF_list sl
       on fl.Docter_ID=PROFESSIONAL_ID__C
)
  
   

--select * from final_list
select flsf.*, 
        CASE
        WHEN flsf.SYNC_ID is NULL and flsf.SYNC_TYPE is NULL and flsf.SYNCED_PRACTICE_MANAGEMENT_SYSTEM is NULL
          THEN flsf.PRACTICE_MANAGEMENT_SYSTEM_LISTED_ON_DSP
        ELSE flsf.SYNCED_PRACTICE_MANAGEMENT_SYSTEM
        END AS PMS,
        CASE
--        WHEN fl.SYNC_TYPE = 'Read/Write/Track'
--          THEN 'True Way'
        WHEN flsf.SYNC_TYPE = 'Read/Write' OR flsf.SYNC_TYPE = 'Read/Write/Track'
          THEN 'Two Way'
        WHEN flsf.SYNC_TYPE = 'Read Only'
          THEN 'One Way'  
        ELSE 'Manual'
        END AS F_Sync_Type
        from final_list_SF flsf

