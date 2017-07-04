from airflow.models import DAG
from datetime import timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.subdag_operator import SubDagOperator

CRAWLER_DIR = '/home/pzwxy/work/AtmanCorp/project/Crawler/'
TRANSLATOR_DIR = 'aaaa'

default_args = {
    'owner': 'pzwxy',
    'depends_on_past': False,
    'start_date': datetime(2017, 6, 29, 20, 32),
    'email': ['pzwxy_77@163.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def subdag(parent_dag_name, child_dag_name, args, **kwargs):
    dag_subdag = DAG(
        dag_id='%s.%s' % (parent_dag_name, child_dag_name),
        default_args=args,
        schedule_interval="@once",
    )

    entry = kwargs['entry']
    saved_folder_name = kwargs['saved_folder_name']
    white_list = kwargs['white_list']
    black_list = kwargs['black_list']
    language = kwargs['language']
    interested_url_regex = kwargs['interested_url_regex']
    parsed_file_name = kwargs['parsed_file_name']
    pre_process_file_name = kwargs['pre_process_file_name']

    t1 = BashOperator(
        task_id='crawler_preparation' + "_" + entry[entry.index('//') + 2: entry.index('.')],
        bash_command=' '.join(
            ['python', CRAWLER_DIR + 'crawler.py ', '-e', entry, '-o', CRAWLER_DIR + saved_folder_name,
             '-wl', white_list, '-bl', black_list, '-ps', 'http://192.168.1.142:3001/api/pop']),
        dag=dag_subdag
    )

    t2 = BashOperator(
        task_id='parser' + "_" + entry[entry.index('//') + 2: entry.index('.')],
        bash_command=' '.join(['python', CRAWLER_DIR + 'parse_site.py', CRAWLER_DIR + saved_folder_name,
                               interested_url_regex, language, CRAWLER_DIR + parsed_file_name]),
        dag=dag_subdag
    )

    t3 = BashOperator(
        task_id='sentence_generator' + "_" + entry[entry.index('//') + 2: entry.index('.')],
        bash_command=' '.join(['python', CRAWLER_DIR + 'pre_process_sentence_before_translation.py',
                               CRAWLER_DIR + parsed_file_name, language, '| sort -u >',
                               CRAWLER_DIR + pre_process_file_name]),
        dag=dag_subdag
    )

    t2.set_upstream(t1)
    t3.set_upstream(t2)

    return dag_subdag

crawler_dag = DAG(
    dag_id='crawler_dag' + '_' + 'airflow',
    default_args=default_args,
    description='DAG for scrape, parse and generate...',
    schedule_interval=None
)

sub_dag = SubDagOperator(
    task_id='crawler',
    subdag=subdag('crawler_dag' + '_' + 'airflow', 'crawler', default_args,
                  entry='http://airflow.apache.org/',
                  saved_folder_name='airflow',
                  white_list='"http://airflow.apache.org/.*"',
                  black_list='"((.*\?.+)|(.*\.jpg$))"',
                  language='en',
                  interested_url_regex='http://airflow.apache.org',
                  parsed_file_name='airflow_home_sentences.txt',
                  pre_process_file_name='airflow_home_sentences.txt.sort.uniq.pre'),
    default_args=default_args,
    dag=crawler_dag,
)

crawler1_dag = DAG(
    dag_id='crawler1_dag' + '_' + 'cnbeta',
    default_args=default_args,
    description='DAG for scrape, parse and generate...',
    schedule_interval=None
)

sub_dag1 = SubDagOperator(
    task_id='crawler1',
    subdag=subdag('crawler1_dag' + '_' + 'cnbeta', 'crawler1', default_args,
                  entry='http://www.cnbeta.com/',
                  saved_folder_name='cnbeta',
                  white_list='"http://www.cnbeta.com/.*"',
                  black_list='"((.*\?.+)|(.*\.jpg$))"',
                  language='en',
                  interested_url_regex='http://www.cnbeta.com',
                  parsed_file_name='cnbeta_home_sentences.txt',
                  pre_process_file_name='cnbeta_home_sentences.txt.sort.uniq.pre'),
    default_args=default_args,
    dag=crawler1_dag,
)

crawler2_dag = DAG(
    dag_id='crawler1_dag' + '_' + 'qq',
    default_args=default_args,
    description='DAG for scrape, parse and generate...',
    schedule_interval=None
)

sub_dag2 = SubDagOperator(
    task_id='crawler1',
    subdag=subdag('crawler1_dag' + '_' + 'qq', 'crawler1', default_args,
                  entry='http://www.cnbeta.com/',
                  saved_folder_name='qq',
                  white_list='"http://www.cnbeta.com/.*"',
                  black_list='"((.*\?.+)|(.*\.jpg$))"',
                  language='en',
                  interested_url_regex='http://www.cnbeta.com',
                  parsed_file_name='cnbeta_home_sentences.txt',
                  pre_process_file_name='cnbeta_home_sentences.txt.sort.uniq.pre'),
    default_args=default_args,
    dag=crawler2_dag,
)

