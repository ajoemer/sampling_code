drop table if exists true_target;
create temp table true_target as (

select szn, total, home_spread,
      round( implied_home*1.0/(implied_home+implied_away) ,7) as home_win,
      round( implied_away*1.0/(implied_home+implied_away) ,7) as away_win,
      round( implied_over*1.0/(implied_over+implied_under) ,7) as over_prob,
      round( implied_under*1.0/(implied_over+implied_under) ,7) as under_prob

from (
select szn, total, home_spread,
       case when home_spread_ml < 0 then 1.0-(100.00/(abs(home_spread_ml)+100.0))
           else 100.0/(home_spread_ml+100.0) end as implied_home,
       case when away_spread_ml < 0 then 1.0-(100.00/(abs(away_spread_ml)+100.0))
           else 100.0/(away_spread_ml+100.0) end as implied_away,
       case when over < 0 then 1.0-(100.00/(abs(over)+100.0))
           else 100.0/(over+100.0) end as implied_over,
        case when under < 0 then 1.0-(100.00/(abs(under)+100.0))
           else 100.0/(under+100.0) end as implied_under
from target ) );
