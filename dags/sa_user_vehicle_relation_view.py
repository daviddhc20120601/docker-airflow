# -*- coding: utf-8 -*-
import time
import airflow.utils.dates
from airflow.operators.bash_operator import BashOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import DAG

# ----------------------------------------------------
dag_name = 'sa_user_vehicle_relation_view'

args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1)
}

dag = DAG(
    dag_id=dag_name,
    default_args=args,
    schedule_interval='0 16 * * *'
)

start = BashOperator(
    task_id='start',
    bash_command="echo start... ...",
    dag=dag)
end = BashOperator(
    task_id='end',
    bash_command="echo end ... ...",
    dag=dag)

create_sa_user_vehicle_relation = BashOperator(
    task_id="create_sa_user_vehicle_relation",
    bash_command="""hive -e "CREATE TABLE if not exists mos.sa_user_vehicle_relation ( idp_user_id string, mos_user_id bigint, vin string, real_name_status boolean) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' LINES TERMINATED BY '\n' STORED AS TEXTFILE;"
""",
    dag=dag)

invalidate_metadata = BashOperator(
    task_id="invalidate_metadata",
    bash_command=""" impala-shell -i svlhdp001.csvw.com -q 'invalidate metadata' 
	""",
    dag=dag)
insert = BashOperator(
    task_id="insert",
    bash_command=""" impala-shell -i svlhdp001.csvw.com -d mos -q "INSERT overwrite table mos.sa_user_vehicle_relation SELECT mos.sa_tm_user_with_cp.open_id AS idp_user_id,mos.sa_tm_users.id AS mos_user_id,vin_user_relation.vin AS vin,vin_user_relation.real_name_status AS real_name_status FROM (SELECT party_id, vin, (CASE WHEN status = 'realnameDone' then true ELSE false END) AS real_name_status,max( to_timestamp(update_time,\"yyyy-MM-dd HH:mm:ss\"))  as last_update_time FROM mos.sa_tr_party_account_role WHERE role_code = 'PRIMARY_USER' AND (status = 'bind' OR status = 'realnameDone' OR status = 'mbbDone') GROUP BY party_id,vin,status) AS vin_user_relation LEFT JOIN mos.sa_tm_users ON vin_user_relation.party_id = mos.sa_tm_users.party_id LEFT JOIN mos.sa_tm_user_with_cp ON (mos.sa_tm_user_with_cp.user_id = mos.sa_tm_users.id AND mos.sa_tm_user_with_cp.app_id = 'svw-idp')"
""",
    dag=dag)

start >> create_sa_user_vehicle_relation >> invalidate_metadata >> insert >> end

# ----------------------------------------------------
if __name__ == "__main__":
    pass
