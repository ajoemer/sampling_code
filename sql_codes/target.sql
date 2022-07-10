drop table if exists target;
create temp table target as (
select
       'over'   as SZN,
         3 as home_spread,
       -109 as home_spread_ml,
        -101 as away_spread_ml,
        210 as total,
        -110 as over,
        -100 as under  );

drop table if exists true_target;
