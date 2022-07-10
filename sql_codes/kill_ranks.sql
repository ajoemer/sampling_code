drop table if exists kill_ranks;
create temp table kill_ranks as (
select *, row_number() over (partition by spread_Result order by pythag_error_rank desc) as spread_kill_rank,
         row_number() over (partition by total_Result order by pythag_error_rank desc) as total_kill_rank
    from ranked_data
        where pythag_error_rank <= 500 and total_result <> 'push' and spread_result <> 'push');




drop table if exists add_ranks;
create temp table add_ranks as (
select *, row_number() over (partition by spread_Result order by pythag_error_rank asc) as spread_add_rank,
         row_number() over (partition by total_Result order by pythag_error_rank asc) as total_add_rank
    from ranked_data
        where pythag_error_rank > 500);




drop table if exists ded_spread;
create temp table ded_spread as (

select k.*
    from kill_ranks k
    join needed n on k.spread_result = 'home_cover' and spread_kill_rank <= -n.home_cover

union

select k.*
    from kill_ranks k
    join needed n on k.spread_result = 'away_cover' and spread_kill_rank <= -n.away_cover);




drop table if exists adj_needed;
create temp table adj_needed as (
select home_cover, away_cover, sum(case when total_result = 'over' then 1 else 0 end) + over as over,
       sum(case when total_result = 'under' then 1 else 0 end) + under as under
from (
        select n.*, d.total_result
        from needed n
           left join ded_spread d on 1=1)
    group by 1,2, over, under);




drop table if exists new_kill_rank;
create temp table new_kill_rank as (
select *, row_number() over (partition by total_result order by pythag_error_rank desc) as new_total_kill_rank
    from kill_ranks
        where game_id not in (select game_id from ded_spread)
    and total_result <> 'Push' and spread_result <> 'Push'
    );



drop table if exists ded_total;
create temp table ded_total as (

select k.*
    from new_kill_rank k
    join adj_needed n on k.total_result = 'over' and  -n.over >= new_total_kill_rank

    where game_id not in (Select game_id from ded_spread)

union

select k.*
    from new_kill_rank k
    join adj_needed n on k.total_result = 'under' and -n.under >= new_total_kill_rank

    where game_id not in (Select game_id from ded_spread) );





drop table if exists final_needed;
create temp table final_needed as (

select home_covers_needed - home_covers_in_sample as home_cover,
       away_covers_needed - away_covers_in_sample as away_cover,
       overs_needed - overs_in_sample as over,
       unders_needed - unders_in_sample as under, 500-sample as total_games_needed

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
       count(r.*) as sample

from  ranked_data r
join sample_spread_median ss  on 1=1
join sample_total_median st on 1=1
where pythag_error_rank <= 500
and r.game_id not in (Select game_id from ded_total)
and r.game_id not in (select game_id from ded_spread)
group by 1,2,3,4,5,6,7,8));






drop table if exists add_ranks;
create temp table add_ranks as (
select *, row_number() over (partition by total_result,spread_Result order by pythag_error_rank asc) as add_rank,
       case when spread_result = 'home_cover' and total_result = 'over' then 'ho'
              when   spread_result = 'away_cover' and total_result = 'over' then 'ao'
           when spread_result = 'home_cover' and total_result = 'under' then 'hu'
              when   spread_result = 'away_cover' and total_result = 'under' then 'au'
             when spread_result = 'push' and total_result = 'push' then 'pp'
            when spread_result = 'push' and total_result = 'over' then 'po'
            when spread_result = 'push' and total_result = 'under' then 'pu'
            when spread_result = 'home_cover' and total_result = 'push' then 'hp'
           when spread_result = 'away_cover' and total_result = 'push' then 'ap'
 else 'drop' end as type
    from ranked_data
        where pythag_error_rank > 500
    and game_id not in (select game_id from ded_spread)
    and game_id not in (select game_id from ded_total));




drop table if exists final_pivot;
create temp table final_pivot as (
select case when under = 0 then home_cover else 0 end  as ho, case when under = 0 then away_cover else 0 end  as ao,
       case when over = 0 then home_cover else 0 end as hu, case when over = 0 then away_cover else 0 end as au
    from final_needed);




drop table if exists final_pivot_2;
create temp table final_pivot_2 as (

select case when away_cover = 0 then over else 0 end  as ho, case when away_cover = 0 then under else 0 end  as hu,
       case when home_cover = 0 then over else 0 end as ao, case when home_cover = 0 then under else 0 end as au
    from final_needed);





drop table if exists final_sample;
create temp table final_sample as (

select distinct game_id
from ranked_data r  where pythag_error_rank <= 500
and r.game_id not in (Select game_id from ded_total)
and r.game_id not in (select game_id from ded_spread)

union

select a.game_id
    from add_ranks a
    join final_pivot f on a.type = 'ho' and f.ho >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot_2 f2 on a.type = 'ho' and f2.ho >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot f on a.type = 'hu' and f.hu >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot_2 f2 on a.type = 'hu' and f2.hu >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot f on a.type = 'ao' and f.ao >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot_2 f2 on a.type = 'ao' and f2.ao >= a.add_rank

union

select a.game_id
    from add_ranks a
    join final_pivot f on a.type = 'au' and f.au >= a.add_rank

    union

select a.game_id
    from add_ranks a
    join final_pivot_2 f2 on a.type = 'au' and f2.au >= a.add_rank);






drop table if exists sample_full;
create temp table sample_full as (
select cdv.*
from final_sample x
join clean_de_vigged cdv on cdv.game_id = x.game_id);






 drop table if exists sample_aggregated_actuals;
create temporary table sample_aggregated_actuals as (
select  t.*, count(*) as sample_Size,
round(sum(case when cdv.home_score + t.home_spread > cdv.away_score then 1 else 0 end)*1.0/sum(case when home_score + t.home_spread <> away_score  then 1 else 0 end),5) as sample_home_win,
      count(distinct case when cdv.home_score + cdv.away_score > t.total then cdv.game_id end)*1.0/
       sum( case when cdv.home_score + cdv.away_score <> t.total then 1 else 0 end) as sample_over

from sample_full cdv
join true_target t on 1=1
group by 1,2,3,4,5,6,7
HAVING sum( case when cdv.home_score + cdv.away_score <> t.total then 1 else 0 end) > 0
    AND sum(case when home_score <> away_score  then 1 else 0 end) > 0
    );





drop table if exists final_final_add;
create temp table final_final_add as (
select add, home_win as home_cover, add-home_win as away_cover, over as over, add-over as under
from (
select  case when sample_Size < 500 then 500- sample_size else 0 end as add,
        case when home_win < sample_home_win then (500-sample_size)/2 + round((home_win-sample_home_win)*(500-sample_size) ,0)
             when home_win = sample_home_win then (500-sample_size)/2
             when home_win > sample_home_win then (500-sample_size)/2 + round((home_win-sample_home_win)*(500-sample_size),0) end as home_win,
        case when over_prob < sample_over then (500-sample_size)/2 + round((over_prob-sample_over)*(500-sample_size) ,0)
             when over_prob = sample_over then (500-sample_size)/2
             when over_prob > sample_over then (500-sample_size)/2 + round((over_prob-sample_over)*(500-sample_size),0) end as over
    from sample_aggregated_actuals) where add > 0);






drop table if exists final_final_chart;
create temp table final_final_chart as (
select  round(home_cover*over*add) as ho,
       round(away_cover*over* add) as ao,
       round(home_cover*under *add) as hu,
       round(away_cover*under*add) as au
       from (

select add, home_cover*1.0/add as home_cover, away_cover*1.0/add as away_cover, over*1.0/add as over, under*1.0/add as under
from final_final_add));





drop table if exists final_final_add_rank;
create temp table final_final_add_rank as (

select *, row_number() over (partition by total_result, spread_result order by pythag_error_rank asc) as final_rank
    from ranked_data r
where game_id not in (select game_id from final_sample)
    and total_result <> 'push' and spread_result <> 'push'
order by pythag_error_rank);






drop table if exists fivehunnit;
create temp table fivehunnit as (

select distinct game_id
    from final_final_add_rank f
     join final_final_chart c on  f.final_rank <= c.ho and f.spread_result = 'home_cover' and f.total_result = 'over'

union

select distinct game_id
    from final_final_add_rank f
     join final_final_chart c on  f.final_rank <= c.ao and f.spread_result = 'away_cover' and f.total_result = 'over'

union

select distinct game_id
    from final_final_add_rank f
     join final_final_chart c on  f.final_rank <= c.au and f.spread_result = 'away_cover' and f.total_result = 'under'

union

select distinct game_id
    from final_final_add_rank f
     join final_final_chart c on  f.final_rank <= c.hu and f.spread_result = 'home_cover' and f.total_result = 'under'

union select *
    from final_sample);





drop table if exists sample_full;
create temp table sample_full as (
select cdv.*
from fivehunnit x
join clean_de_vigged cdv on cdv.game_id = x.game_id
 );





drop table if exists new_sample_aggregated_actuals;
create temporary table new_sample_aggregated_actuals as (
select  t.*, count(*) as sample_Size,
round(sum(case when cdv.home_score + t.home_spread > cdv.away_score then 1 else 0 end)*1.0/sum(case when home_score + t.home_spread <> away_score  then 1 else 0 end),5) as sample_home_win,
      count(distinct case when cdv.home_score + cdv.away_score > t.total then cdv.game_id end)*1.0/
       sum( case when cdv.home_score + cdv.away_score <> t.total then 1 else 0 end) as sample_over, avg(home_margin) as avgspred, avg(points) as avgtotal

from sample_full cdv
join true_target t on 1=1
group by 1,2,3,4,5,6,7
HAVING sum( case when cdv.home_score + cdv.away_score <> t.total then 1 else 0 end) > 0
    AND sum(case when home_score <> away_score  then 1 else 0 end) > 0
    );

