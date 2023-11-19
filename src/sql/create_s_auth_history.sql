drop table if exists STV202310069__DWH.s_auth_history cascade;

create table STV202310069__DWH.s_auth_history (
  hk_l_user_group_activity integer, 
  user_id_from int, 
  event varchar(10), 
  event_dt timestamp, 
  load_dt timestamp not null, 
  load_src VARCHAR(20)
);
ALTER TABLE STV202310069__DWH.s_auth_history 
ADD CONSTRAINT fk_auth_hist FOREIGN KEY (hk_l_user_group_activity) references STV202310069__DWH.l_user_group_activity(hk_l_user_group_activity);