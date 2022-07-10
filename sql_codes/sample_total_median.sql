drop table if exists sample_total_median;
create temp table sample_total_median as (
select median(r.points) as sample_total, t.total as target_total, t.over_prob as target_over, sum(case when spread_result = 'push' then 1 else 0 end) as spread_push
from ranked_data r
join true_target t on 1=1
where pythag_error_rank <= 500
group by 2,3);
