## template dag file for dsp report hourly jobs 

from airflow.operators import BashOperator,MySqlOperator,PythonOperator
from airflow.operators import DummyOperator
from airflow.operators import HdfsSensor, S3KeySensor
from airflow.hooks import MySqlHook
from airflow.models import DAG
from datetime import datetime, timedelta
from dateutil import parser
import os
import logging
from airflow import settings

##utc time

# start_date_hourly_rounded = datetime.strptime(datetime.strftime(datetime.now(), "%Y-%m-%d %H"), "%Y-%m-%d %H")\
# 								- timedelta(0, 60*60*2)

start_date_hourly_rounded = datetime.strptime("2016-06-30 23", "%Y-%m-%d %H")

args = {'owner':'dsp',
		'start_date':start_date_hourly_rounded,
		'retry_delay': timedelta(0, 60),
		# 'retries':5,
		# 'provide_context':True,
		'email':'yangyao@limei.com'
		}

command = ''' echo "this is test command, need to be modified when production " '''
hdfs_path = '/user/yangyao/tag'

##tag file path
winbid_tag_file="/tmp/hk_event_scpdata_tags/winbid/{day}/{hour}/winbid_SUCCESS"
imp_tag_file="/tmp/hk_event_scpdata_tags/impression/{day}/{hour}/impression_SUCCESS"
click_tag_file = "/tmp/hk_event_scpdata_tags/click/{day}/{hour}/click_SUCCESS"
bidreq_tag_file = "/tmp/hk_scpdata_tags/req/{day}/{hour}/req_SUCCESS"
bidresponse_tag_file = "/tmp/hk_scpdata_tags/response/{day}/{hour}/response_SUCCESS"
s3_scheme = "s3://awsdsprpt/"


##connection id
hdfs_conn_id = 'hdfs_conn1' ##sensor local tag file
aws_s3_conn_id = 's3_connection_id' ##sensor aws tag file
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
hour_interval_between_execution_and_now = '{% set now_s = macros.time.mktime(macros.datetime.now().timetuple()) %} \
	{% set exe_s = macros.time.mktime(execution_date.timetuple()) %} \
	{% set interval_hour = (now_s - exe_s)/3600 %} \
	{{ interval_hour|int }}'

# execution_date=parser.parse({{ ts }})

dsp_report_dag = DAG(dag_id='dsp-report-hourly', 
				default_args=args, 
				start_date = start_date_hourly_rounded,
				schedule_interval='0 * * * *')

start_task = DummyOperator(task_id='start_now',
						  dag = dsp_report_dag)
end_task = DummyOperator(task_id='end_here',
	dag=dsp_report_dag,
	trigger_rule='all_done',
	)

XCOM_TASK_STATE_KEY='task_state'

def push_task_state_xcom(kargs):
	##push task state to xcom
	ti = kargs['ti']
	state = ti.state()
	execution_date = ti.execution_date
	logging.info("xcom_push task_instance is %s, state is %s, execution_date is %s"%(str(ti), str(state), str(execution_date)))
	ti.xcom_push(key=XCOM_TASK_STATE_KEY, value=state, execution_date=execution_date )

##gen hdfs_sensor_task
def gen_hdfs_sensor_task(task_id_str = '', hdfs_path=hdfs_path, hdfs_conn_id=hdfs_conn_id,
	parent_dag = dsp_report_dag):
	s_task = HdfsSensor(task_id = task_id_str,
					  filepath = hdfs_path,
					  hdfs_conn_id = hdfs_conn_id,
					  dag = parent_dag,
					  retry_delay = timedelta(0,60), ## retry delay is 1min / 60s
					  retries = 10, ## max retries 5 
					  )
	s_task.set_upstream(start_task)
	return s_task

##gen aws_sensor_task
def gen_s3_sensor_task(task_id_str='',
	s3_scheme = s3_scheme,
	aws_path = hdfs_path,
	s3_connid = aws_s3_conn_id,
	parent_dag = dsp_report_dag
 	):
	s3_sensor_task = S3KeySensor(task_id = task_id_str,
		bucket_key = s3_scheme + aws_path, ## aws s3 need bucket name
		s3_conn_id = s3_connid,
		dag = parent_dag,
		retry_delay = timedelta(0,60), ## retry delay is 1min / 60s
		retries = 5, ## max retries 5 
		)
	s3_sensor_task.set_upstream(start_task)
	return s3_sensor_task


def gen_report_task(task_id_str = '', command = command, parent_dag = dsp_report_dag):
	r_task = BashOperator(
						#task_id = ('local_' if is_local else 'aws_') + task_id_str,
						  task_id = task_id_str,
						  bash_command = command,
						  dag = parent_dag,
						  # on_success_callback=push_task_state_xcom,
						  # on_failure_callback=push_task_state_xcom,
						  )
	return r_task


##python callable for gen_record_success_task
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


##record success job, for high level job sensor
def gen_record_success_task(upstream_task_id_str = '',  
	mysql_conn_id_value = mysql_conn_Id,\
	table_name = success_job_table,
	report_name_value = 'your report_name', 
	report_time_type_value = '0', 
	report_time_value = '1900-01-01',
	success_time_value = '1900-01-01',
	parent_dag = dsp_report_dag
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

## local scptag sensors
local_winbid_scptag_sensor = gen_hdfs_sensor_task(task_id_str = 'local_winbid_scptag_sensor' ,
	# hdfs_path = winbid_tag_file.format(day=datetime.strftime(parser.parse("{{ ts }}"), "%Y-%m-%d"),\
	hdfs_path = winbid_tag_file.format(day= "{{ ds }}",\
		hour="{{ execution_date.strftime(\'%H\') }}" )
	)
local_imp_scptag_sensor = gen_hdfs_sensor_task(task_id_str = 'local_imp_scptag_sensor',
	hdfs_path = imp_tag_file.format(day = "{{ ds }}", hour = '{{ execution_date.strftime(\'%H\') }}')
	)
local_clk_scptag_sensor = gen_hdfs_sensor_task(task_id_str = 'local_clk_scptag_sensor',
	hdfs_path = click_tag_file.format(day = "{{ ds }}", hour = '{{ execution_date.strftime(\'%H\') }}')
	)
local_bidreq_scptag_sensor = gen_hdfs_sensor_task(task_id_str = 'local_bidreq_scptag_sensor',
	hdfs_path = bidreq_tag_file.format(day = "{{ ds }}", hour = "{{ execution_date.strftime(\'%H\') }}")
	)
local_bidresponse_scptag_sensor = gen_hdfs_sensor_task(task_id_str = 'local_bidresponse_scptag_sensor',
	hdfs_path = bidresponse_tag_file.format(day = "{{ ds }}", hour = "{{ execution_date.strftime(\'%H\') }}")
	)

##aws scptag snesors
aws_winbid_scptag_sensor = gen_s3_sensor_task(task_id_str = 'aws_winbid_scptag_sensor',
	aws_path = winbid_tag_file.format(day = '{{ ds }}', hour = '{{ execution_date.strftime(\'%H\') }}')
	)
aws_imp_scptag_sensor = gen_s3_sensor_task(task_id_str = 'aws_imp_scptag_sensor',
	aws_path = imp_tag_file.format(day = '{{ ds }}', hour = '{{ execution_date.strftime(\'%H\') }}')
	)
aws_clk_scptag_sensor = gen_s3_sensor_task(task_id_str = 'aws_clk_scptag_sensor',
	aws_path = click_tag_file.format(day = '{{ ds }}', hour = '{{ execution_date.strftime(\'%H\') }}')
	)
aws_bidreq_scptag_sensor = gen_s3_sensor_task(task_id_str = 'aws_bidreq_scptag_sensor',
	aws_path = bidreq_tag_file.format(day = '{{ ds }}', hour = '{{ execution_date.strftime(\'%H\') }}')
	)
aws_bidresponse_scptag_sensor = gen_s3_sensor_task(task_id_str = 'aws_bidresponse_scptag_sensor',
	aws_path = bidresponse_tag_file.format(day = '{{ ds }}', hour = '{{ execution_date.strftime(\'%H\') }}')
	)
 
##report job task 
test_option='t'  ## test option for repoirt script, "-t"

##linux user to execute report job
LINUX_USER = "mapred2"


##taskid
local_datavisual_hourly_taskid = 'local_hourly_datavisual'
local_hourly_dsp_simple_taskid = 'local_hourly_dsp_simple'
local_hourly_dsp_info_taskid = 'local_hourly_dsp_info'
aws_hourly_dsp_simple_taskid = 'aws_hourly_dsp_simple'
aws_hourly_dsp_info_taskid = 'aws_hourly_dsp_info'

command_template = "sudo -u {username} bash -c \"cd {dir} && bash {script} {paras} \""
# command_template = "echo \"pretending runing...\" "

local_datavisual_hourly = gen_report_task(task_id_str = local_datavisual_hourly_taskid,
	command = command_template.format(username=LINUX_USER,\
		dir = dir_datavisual,script = script_datavisual, \
		paras="{{ ds }}/{{ execution_date.strftime(\'%H\') }} ")
	)
local_dsp_simple_hourly = gen_report_task(task_id_str = local_hourly_dsp_simple_taskid,
	command = command_template.format(username=LINUX_USER,\
		dir = dir_dsp_simple, script = script_dsp_simple,\
		paras='-'+test_option + "h %s "%(hour_interval_between_execution_and_now))	
	)
local_dsp_info_hourly = gen_report_task(task_id_str = local_hourly_dsp_info_taskid,
	command = command_template.format(username=LINUX_USER,\
		dir = dir_dsp_info, script = script_dsp_info,\
		paras='-' + test_option + "h %s "%(hour_interval_between_execution_and_now))
	)
aws_dsp_simple_hourly = gen_report_task(task_id_str = aws_hourly_dsp_simple_taskid,
	command = command_template.format(username=LINUX_USER,\
		dir = dir_dsp_simple, script = script_dsp_simple,\
		paras='-' + test_option + "a %s "%(hour_interval_between_execution_and_now))
	)
aws_dsp_info_hourly = gen_report_task(task_id_str = aws_hourly_dsp_info_taskid,
	command = command_template.format(username=LINUX_USER,\
		dir = dir_dsp_info, script = script_dsp_info,\
		paras='-' + test_option + "a %s "%(hour_interval_between_execution_and_now))
	)

##update success job task
local_dsp_simple_hourly_update_status = gen_record_success_task(upstream_task_id_str = \
	local_hourly_dsp_simple_taskid,
	report_name_value='local_dsp-simple',
	report_time_type_value='hourly',
	report_time_value="{{ ds }}" + " {{ execution_date.strftime(\'%H\') }}:00:00",
	success_time_value="{{ macros.datetime.now() }}"
	)
local_dsp_info_hourly_update_status = gen_record_success_task(upstream_task_id_str = \
	local_hourly_dsp_info_taskid,
	report_name_value="local_dsp-info",
	report_time_type_value="hourly",
	report_time_value="{{ ds }}" + " {{ execution_date.strftime(\'%H\') }}:00:00",
	success_time_value="{{ macros.datetime.now() }}"
	)
aws_dsp_simple_hourly_update_status = gen_record_success_task(upstream_task_id_str = \
	aws_hourly_dsp_simple_taskid,
	report_name_value="aws_dsp-simple",
	report_time_type_value="hourly",
	report_time_value="{{ ds }}" + " {{ execution_date.strftime(\'%H\') }}:00:00",
	success_time_value="{{ macros.datetime.now() }}"
	)
aws_dsp_info_hourly_update_status = gen_record_success_task(upstream_task_id_str=\
	aws_hourly_dsp_info_taskid,
	report_name_value="aws_dsp-info",
	report_time_type_value="hourly",
	report_time_value="{{ ds }}" + " {{ execution_date.strftime(\'%H\') }}:00:00",
	success_time_value="{{ macros.datetime.now() }}"
	)

##set up and down stream
local_dsp_simple_hourly.set_upstream([local_winbid_scptag_sensor, local_imp_scptag_sensor, local_clk_scptag_sensor])
local_dsp_simple_hourly.set_downstream(local_dsp_simple_hourly_update_status)
local_dsp_info_hourly.set_upstream([local_bidreq_scptag_sensor, local_bidresponse_scptag_sensor])
local_dsp_info_hourly.set_downstream(local_dsp_info_hourly_update_status)
aws_dsp_simple_hourly.set_upstream([aws_winbid_scptag_sensor, aws_imp_scptag_sensor, aws_clk_scptag_sensor])
aws_dsp_simple_hourly.set_downstream(aws_dsp_simple_hourly_update_status)
aws_dsp_info_hourly.set_upstream([aws_bidreq_scptag_sensor, aws_bidresponse_scptag_sensor])
aws_dsp_info_hourly.set_downstream(aws_dsp_info_hourly_update_status)
local_datavisual_hourly.set_upstream([aws_dsp_info_hourly, local_dsp_info_hourly])
end_task.set_upstream([local_dsp_info_hourly_update_status, 
	local_dsp_simple_hourly_update_status,
	aws_dsp_info_hourly_update_status,
	aws_dsp_simple_hourly_update_status,
	local_datavisual_hourly
	])

if __name__ == "__main__":
	dsp_report_dag.cli()
