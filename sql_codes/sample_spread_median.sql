drop table if exists sample_spread_median;
create temp table sample_spread_median as
(
    select x.*, count(r.*) as minority_in_sample, sum(case when total_result = 'push' then 1 else 0 end) as total_push
   from (

select -median(r.home_margin)   as sample_home_spread, t.home_spread as target_home_spread, t.home_win as target_home_cover
from ranked_data r
join true_target t on 1=1
where pythag_error_rank <= 500
group by 2,3)  x join ranked_data r on r.pythag_error_rank <= 500
group by 1,2,3);
