create temp table v2 as
select
  b.practice_id,
  a.sync_pms,
  a.sync_type
from cistern_experimental.experimental.provider_location_sync_snapshot_day_new a
join provider.provider b
  on a.provider_id = b.provider_id
where a.snapshot_date_eastern between '2023-12-01' and '2023-12-31'  //Change dates here
  and a.sync_pms in ('Dentrix', 'DentrixG5')
qualify row_number() over (partition by b.practice_id order by a.snapshot_date_eastern desc) = 1
;

select
  a.practice_id,
  b.practice_name,
  c.address1,
  c.address2,
  c.address3,
  c.city,
  c.state,
  c.zip_code,c.phone,
  b.account_segment,
  a.sync_pms,
  a.sync_type
from v2 a
join provider.practice_derived_metrics b
  on a.practice_id = b.practice_id
join provider.location c
  on b.first_location_id = c.location_id;

