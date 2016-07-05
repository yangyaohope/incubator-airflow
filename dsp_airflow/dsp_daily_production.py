##template daily job

from airflow.operators import BashOperator,MySqlOperator, PythonOperator
from airflow.operators import DummyOperator
from airflow.operators import HdfsSensor, S3KeySensor, SqlSensor
from airflow.models import DAG
from airflow.hooks import MySqlHook
from datetime import datetime, timedelta
from dateutil import parser
import os
import logging
from airflow import settings


# start_date_daily_rounded = datetime.combine(datetime.today() -timedelta(1), datetime.min.time())
start_date_daily_rounded = datetime.strptime("2016-06-30 23", "%Y-%m-%d %H")

args = {'owner':'dsp',
		'start_date': start_date_daily_rounded,
		# 'retry_delay': timedelta(0, 60), ##20 minutes for daily job retry interval
		# 'retries':5,
		# 'provide_context':True,
		'email':'yangyao@limei.com',
		}


command = ''' echo "this is test command, need to be modified when production "'''		

##connection id
mysql_conn_Id = 'report_job_status_table_conn_id' ##save job status, for high leval job sensor
success_job_table = 'success_report'

##dir and script file path
project_dir = "/home/mapred2/dsp/yytest/" ##test project dir

dir_dsp_simple = os.path.join( project_dir,"dsp-simple")
script_dsp_simple = "run_simple.sh"
dir_dsp_info = os.path.join(project_dir, "dsp-info")
script_dsp_info = "run.sh"
dir_datavisual = os.path.join(project_dir, "datavisual")
script_datavisual = "run.sh"
dir_dsp_uv = os.path.join(project_dir, "dsp-uv")
script_dsp_uv = "run.sh"
dir_landing_page = os.path.join(project_dir, "landing-page")
script_landing_page = "run.sh"
dir_userplatform = os.path.join(project_dir, "up")
script_userplatform = "userplatform-daily-task.sh"
dir_extract_applist = os.path.join(project_dir, "extract_applist")
script_extract_applist = "extract-applist.sh"
dir_media_stats = os.path.join(project_dir, "media_stats")
script_media_stats = "media-daily-stats.sh"

##interval day between now and execution date day
day_interval_between_execution_and_now = '{% set now_s = macros.time.mktime(macros.datetime.now().timetuple()) %} \
	{% set exe_s = macros.time.mktime(execution_date.timetuple()) %} \
	{% set interval_day = (now_s - exe_s)/(3600*24) %} \
	{{ interval_day|int }}'

dag = DAG(dag_id = 'dsp-report-daily',
	default_args = args,
	start_date = start_date_daily_rounded,
	schedule_interval = '0 0 * * *',
	)

start_task = DummyOperator(task_id = 'start_now',
	dag = dag)
end_task = DummyOperator(task_id = 'end_here',
	dag = dag,
	trigger_rule='all_done',)

def gen_hourly_job_sensor(report_name_value = 'your_report_name', 
							task_id_value = None,
							report_time_type_value = 'hourly',
							report_time_day_value = '1970-01-01',
							mysql_connid_value = mysql_conn_Id,
							table_value = success_job_table,
							parent_dag=dag,
							):
	sql_template = "select case when count(*) >=24 \
	then 1 else 0 end  from {table} \
	where report_name = '{report_name}' \
	and report_time_type = '{report_time_type}' \
	and report_time between '{report_time_begin}' \
	and '{report_time_end}';"
	# day = (parser.parse(report_time_day_value)).strftime("%Y-%m-%d")
	day = report_time_day_value
	day_begin = day + " 00:00:00"
	day_end = day + " 23:59:59"

	if not task_id_value:
		task_id_value = 'sensor_%s_%s'%(report_name_value, report_time_type_value)

	sql = sql_template.format(table=table_value,
		report_name=report_name_value,
		report_time_type=report_time_type_value,
		report_time_begin=day_begin,
		report_time_end=day_end)
	hourly_job_sensor_task = SqlSensor(task_id=task_id_value,
		dag = parent_dag,
		conn_id=mysql_connid_value,
		sql=sql,
		retry_delay = timedelta(0,60), ## retry delay is 1min / 60s
		retries = 10, ## max retries 5 
		)
	return hourly_job_sensor_task


def gen_daily_report_task(task_id_value = '',
	command_value=command,
	parent_dag = dag,
	):
	
	daily_report_task = BashOperator(task_id=task_id_value,
		dag=parent_dag,
		bash_command=command_value,
		)
	return daily_report_task


def insert_or_delete_task_state(*args, **kargs):
	ti = kargs['ti'] ##current task instance
	insert_sql = kargs['insert_sql'] #insert upstream state sql
	delete_sql = kargs['delete_sql'] #delete upstream state sql
	upstream_task_id = kargs['up_id'] #upstream task id
	xcom_pull_key = kargs['xcom_key'] #key for xcom_pull
	mysql_conn_id = kargs['mysql_conn_id'] #conn id for database which stores state table

	actual_sql = ''
	##method 1 of get the upstream task's state
	# up_state = ti.xcom_pull(key=xcom_pull_key, task_ids = upstream_task_id)

	upstream_task_list = kargs['task'].upstream_list
	logging.info('%s upstream task list is %s'%(kargs['task'], upstream_task_list))
	upstream_task_state = upstream_task_list[0].get_task_instances(session=settings.Session(),\
		start_date=kargs['execution_date'],\
		end_date=kargs['execution_date'])[0].state
	logging.info("%s upstream task(%s) state is %s"%(kargs['task'], upstream_task_list[0], upstream_task_state))
	if upstream_task_state == 'success':
		actual_sql = insert_sql
	elif upstream_task_state == 'failed':
		actual_sql = delete_sql
	else:
		actual_sql = 'show databases;'	## if not success or failed, do something effectiveless
	logging.info("upstream state is %s, actual update status sql is %s"%(str(upstream_task_state), str(actual_sql) ))
	##start to execute sql
	logging.info('Executing: %s'%(str(actual_sql)))
	hook=MySqlHook(mysql_conn_id=mysql_conn_id)
	hook.run(actual_sql)	

XCOM_TASK_STATE_KEY='task_state'

def gen_update_job_status_task(upstream_task_id_str=None,
	mysql_conn_id_value=mysql_conn_Id,
	table_name=success_job_table,
	report_name_value = 'your report_name', 
	report_time_type_value = 'daily', 
	report_time_value = '1900-01-01',
	success_time_value = '1900-01-01',
	parent_dag = dag,
	):
	
	upstream_task_id = upstream_task_id_str
	task_id_str = "update_status_%s"%(upstream_task_id_str)

	insert_sql_template = "insert into {table} values({report_name},{report_time_type},\
		{report_time},{success_time}) on duplicate key update success_time = {success_time}"
	insert_sql = insert_sql_template.format(table = "" + table_name + "",
		report_name = "'" + report_name_value + "'",
		report_time_type = "'" + report_time_type_value + "'",
		report_time = "'" + report_time_value + "'",
		success_time = "'" + success_time_value + "'"
		)
	# print "update success job record sql is: " + insert_sql

	delete_sql_template = "delete from {table} where report_name={report_name}\
	and report_time={report_time}\
	and report_time_type={report_time_type}"
	delete_sql = delete_sql_template.format(table=table_name, 
		report_name = "'" + report_name_value + "'",
		report_time = "'" + report_time_value + "'",
		report_time_type = "'" + report_time_type_value + "'",
		)

	insert_success_job_task = PythonOperator(task_id = task_id_str,
		dag = parent_dag,
		python_callable=insert_or_delete_task_state,
		op_kwargs={'insert_sql':insert_sql, 'delete_sql':delete_sql, 'up_id':upstream_task_id_str,
					'xcom_key':XCOM_TASK_STATE_KEY, 'mysql_conn_id':mysql_conn_id_value,},
		trigger_rule='all_done',
		provide_context=True,
		)
	insert_success_job_task.__class__.template_fields += ('op_kwargs', ) ##must, if not, sql will not be rendered
	return insert_success_job_task	
 
##report name macros
LOCAL = "local"
AWS = "aws"
DSP_INFO = "dsp-info"
DSP_SIMPLE = "dsp-simple"

##linux user for execute job report computing
LINUX_USER = "mapred2"

##execution date day string
execution_date_day_str = "{{ execution_date.strftime(\"%Y-%m-%d\") }}"
test_option='t'  ## test option for repoirt script, "-t"


##hourly job sensor task
is_local_dsp_simple_hourly_all_ready = gen_hourly_job_sensor(report_name_value="%s_%s"%(LOCAL, DSP_SIMPLE),
	report_time_day_value=execution_date_day_str
	)
is_local_dsp_info_hourly_all_ready = gen_hourly_job_sensor(report_name_value="%s_%s"%(LOCAL, DSP_INFO),
	report_time_day_value=execution_date_day_str
	)
is_aws_dsp_simple_hourly_all_ready = gen_hourly_job_sensor(report_name_value="%s_%s"%(AWS, DSP_SIMPLE),
	report_time_day_value=execution_date_day_str
	)
is_aws_dsp_info_hourly_all_ready = gen_hourly_job_sensor(report_name_value="%s_%s"%(AWS, DSP_INFO),
	report_time_day_value=execution_date_day_str
	)

##report job tasks
local_dsp_uv_daily_taskid = "local_dsp_uv_daily"
local_dsp_simple_daily_taskid = "local_dsp_simple_daily"
local_dsp_info_daily_taskid = "local_dsp_info_daily"
landing_page_taskid = "landing_page"
local_join_taskid = "local_join"
local_userplatform_daily_taskid = "local_userplatform_daily"
local_media_stat_daily_taskid = "local_media_stat_daily"
local_extract_applist_daily_taskid = "local_extract_applist_daily"
aws_join_taskid = "aws_join"
aws_userplatform_daily_taskid = "aws_userplatform_daily"
aws_mediastat_daily_taskid = "aws_mediastat_daily"
aws_extract_applist_daily_taskid = "aws_extract_applist_daily"
datavisual_daily_taskid = "datavisual_daily"

##command template
command_template = "sudo -u {username} bash -c \"cd {dir} && bash {script} {paras} \""

local_dsp_uv_daily = gen_daily_report_task(task_id_value=local_dsp_uv_daily_taskid,
	command_value=command_template.format(username=LINUX_USER, 
		dir=dir_dsp_uv,
		script=script_dsp_uv,
		paras=" -d {{ ds }}"
		)
	)

local_dsp_simple_daily = gen_daily_report_task(task_id_value=local_dsp_simple_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_dsp_simple,
		script=script_dsp_simple,
		paras=" -%sd %s"%(test_option, day_interval_between_execution_and_now))
	)

local_dsp_info_daily = gen_daily_report_task(task_id_value=local_dsp_info_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_dsp_info,
		script=script_dsp_info,
		paras=" -%sd %s"%(test_option, day_interval_between_execution_and_now))
	)

landing_page = gen_daily_report_task(task_id_value=landing_page_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_landing_page,
		script=script_landing_page,
		paras=" daily {{ ds }}")
	)

local_join = gen_daily_report_task(task_id_value=local_join_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_dsp_info,
		script=script_dsp_info,
		paras=" -%sj %s"%(test_option, day_interval_between_execution_and_now))
	)

local_userplatform_daily = gen_daily_report_task(task_id_value=local_userplatform_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_userplatform,
		script=script_userplatform,
		paras=" {{ ds }}")
	)

local_media_stat_daily = gen_daily_report_task(task_id_value=local_media_stat_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_media_stats,
		script=script_media_stats,
		paras=" {{ ds }}")
	)

local_extract_applist_daily = gen_daily_report_task(task_id_value=local_extract_applist_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_extract_applist,
		script=script_extract_applist,
		paras=" {{ ds }}")
	)



aws_join = gen_daily_report_task(task_id_value=aws_join_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_dsp_info,
		script=script_dsp_info,
		paras=" -%sk %s"%(test_option, day_interval_between_execution_and_now))
	)

aws_userplatform_daily = gen_daily_report_task(task_id_value=aws_userplatform_daily_taskid)
aws_mediastat_daily = gen_daily_report_task(task_id_value=aws_mediastat_daily_taskid)
aws_extract_applist_daily = gen_daily_report_task(task_id_value=aws_extract_applist_daily_taskid)

datavisual_daily = gen_daily_report_task(task_id_value=datavisual_daily_taskid,
	command_value=command_template.format(username=LINUX_USER,
		dir=dir_datavisual,
		script=script_datavisual,
		paras=" {{ ds }} DAILY")
	)


##update job status tasks
update_datavisual_daily_status = gen_update_job_status_task(upstream_task_id_str=datavisual_daily_taskid,
	report_name_value='datavisual',
	report_time_type_value='daily',
	report_time_value="{{ ds }}",
	success_time_value="{{ macros.datetime.now() }}"
	)
update_local_userplatform_daily_status = gen_update_job_status_task(upstream_task_id_str=\
		local_userplatform_daily_taskid,
	report_name_value='local_userplatform',
	report_time_type_value='daily',
	report_time_value="{{ ds }}",
	success_time_value="{{ macros.datetime.now() }}"
	)
update_aws_userplatform_daily_status = gen_update_job_status_task(upstream_task_id_str=\
		aws_userplatform_daily_taskid,
	report_name_value='aws_userplatform',
	report_time_type_value='daily',
	report_time_value="{{ ds }}",
	success_time_value="{{ macros.datetime.now() }}")


##set dependencies
start_task.set_downstream([is_local_dsp_info_hourly_all_ready, \
	is_local_dsp_simple_hourly_all_ready,
	is_aws_dsp_simple_hourly_all_ready,
	is_aws_dsp_info_hourly_all_ready])

#local dsp uv
local_dsp_uv_daily.set_upstream(is_local_dsp_simple_hourly_all_ready)
local_dsp_uv_daily.set_downstream(end_task)

#local dsp simple daily
local_dsp_simple_daily.set_upstream(is_local_dsp_simple_hourly_all_ready)
local_dsp_simple_daily.set_downstream(end_task)

#local dsp info daily
local_dsp_info_daily.set_upstream(is_local_dsp_simple_hourly_all_ready)
local_dsp_info_daily.set_downstream(end_task)

#landing page
landing_page.set_upstream(is_local_dsp_simple_hourly_all_ready)
landing_page.set_downstream(datavisual_daily)

#local join
local_join.set_upstream([is_local_dsp_info_hourly_all_ready, is_local_dsp_simple_hourly_all_ready])
local_join.set_downstream(datavisual_daily)

#aws join
aws_join.set_upstream([is_aws_dsp_info_hourly_all_ready, is_aws_dsp_simple_hourly_all_ready])
aws_join.set_downstream(datavisual_daily)

#datavisual daily
datavisual_daily.set_downstream(update_datavisual_daily_status)
update_datavisual_daily_status.set_downstream(end_task)

#local userplatform daily
local_userplatform_daily.set_upstream(is_local_dsp_info_hourly_all_ready)
local_userplatform_daily.set_downstream(update_local_userplatform_daily_status)
update_local_userplatform_daily_status.set_downstream(end_task)

#local extract applist
local_extract_applist_daily.set_upstream(is_local_dsp_info_hourly_all_ready)
local_extract_applist_daily.set_downstream(end_task)

#local media stats
local_media_stat_daily.set_upstream(is_local_dsp_info_hourly_all_ready)
local_media_stat_daily.set_downstream(end_task)

#aws userplateform
aws_userplatform_daily.set_upstream(is_aws_dsp_info_hourly_all_ready)
aws_userplatform_daily.set_downstream(update_aws_userplatform_daily_status)
update_aws_userplatform_daily_status.set_downstream(end_task)

#aws extract applist
aws_extract_applist_daily.set_upstream(is_aws_dsp_info_hourly_all_ready)
aws_extract_applist_daily.set_downstream(end_task)

#aws media stat 
aws_mediastat_daily.set_upstream(is_aws_dsp_info_hourly_all_ready)
aws_mediastat_daily.set_downstream(end_task)



if "__name__" == "__main__":
	dag.cli()