drop table if exists ranked_data;
create temp table ranked_data as (
    select x.*, row_number() over (partition by true order by pythag_error asc)  as pythag_error_rank
    from (

    select c.szn, c.game_id, c.spread_home, (c.spread_home - t.home_spread)^2 +  1.5*(c.total - t.total)^2
         +.00001*datediff(days, c.game_date, current_date) as pythag_error
         , points, home_margin, home_score, away_Score, case when points > t.total then 'over'
                                                             when points < t.total then 'under'
                                                             else 'push' end as total_result,
                                                       case when home_score + t.home_spread -away_score > 0 then 'home_cover'
                                                             when home_score + t.home_spread -away_score < 0 then 'away_cover'
                                                             else 'push' end as spread_result
        from clean_de_vigged c
        join true_target t on c.szn = t.szn) x
        order by 11 asc);
