drop table if exists needed;
create temp table needed as (

select home_covers_needed - home_covers_in_sample as home_cover,
       away_covers_needed - away_covers_in_sample as away_cover,
       overs_needed - overs_in_sample as over,
       unders_needed - unders_in_sample as under

from (
select ss.target_home_spread,
    round(ss.target_home_cover*(500-spread_push) ,0) as home_covers_needed,
       round((1-ss.target_home_cover)*(500-spread_push), 0) as away_covers_needed,
       st.target_total,
       round(st.target_over*(500-total_push),0)   as overs_needed,
       round((1-st.target_over)*(500-total_push),0)   as unders_needed,
       st.spread_push,
       ss.total_push,
       sum(case when spread_result = 'home_cover' then 1 else 0 end) as home_covers_in_sample,
       sum(case when spread_result = 'away_cover' then 1 else 0 end) as away_covers_in_sample,
       sum(case when total_result = 'over' then 1 else 0 end) as overs_in_sample,
       sum(case when total_result = 'under' then 1 else 0 end) as unders_in_sample,
       count(r.*)

from sample_spread_median ss
join sample_total_median st on 1=1
join ranked_data r on pythag_error_rank <= 500
group by 1,2,3,4,5,6,7,8) );
