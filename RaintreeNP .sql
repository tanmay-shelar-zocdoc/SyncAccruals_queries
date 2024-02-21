SET(monthStart, monthEnd)=('2023-05-01', '2023-06-01');
select distinct pp.provider_id, pp.first_name, pp.last_name, pr.name, so.sync_type, max(oi.number_of_new_patient_timeslots_28_days) as NPslots, max(oi.number_of_existing_patient_timeslots_28_days) as EPslots, count(distinct app.appointment_id) as NPBookingCount, 'HadNPBooking' as Bucket
from provider.location pl
join provider.provider_location_mapping plm on pl.location_id = plm.location_id
join provider.provider pp on plm.provider_id = pp.provider_id
join provider.practice pr on pp.practice_id = pr.practice_id
join sync.sync_overview_vw so on pl.location_id = so.location_id
left outer join provider_analytics.OPPORTUNITY_INDEX oi on pp.monolith_professional_id = oi.monolith_professional_id
join appointment.appointment_summary app on pp.provider_id = app.provider_id
where pp.latest_activation_start_timestamp_utc < $monthEnd
and (pp.latest_activation_end_timestamp_utc is null or pp.latest_activation_end_timestamp_utc > $monthEnd)
and pl.software ilike '%raintree%' 
and oi.snapshot_date_est between $monthStart and dateadd(day, 1, $monthStart)
and app.appointment_created_timestamp_local between $monthStart and $monthEnd
and app.is_premium_booking = true
group by pp.provider_id, pp.first_name, pp.last_name, pr.name, so.sync_type
order by 1 desc;
