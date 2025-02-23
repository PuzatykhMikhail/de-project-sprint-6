with user_group_log as 
(
  select hg.hk_group_id, 
    	 hg.registration_dt, 
    	 count(DISTINCT luga.hk_user_id) as cnt_added_users 
  from STV202310069__DWH.h_groups as hg 
  left join STV202310069__DWH.l_user_group_activity as luga on luga.hk_group_id = hg.hk_group_id 
  left join STV202310069__DWH.s_auth_history as sah on sah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
  where sah.event = 'add' 
  group by hg.hk_group_id, 
    	   hg.registration_dt 
  order by hg.registration_dt 
  limit 10
) 
select hk_group_id, 
  	   cnt_added_users 
from user_group_log 
order by cnt_added_users 
limit 10;