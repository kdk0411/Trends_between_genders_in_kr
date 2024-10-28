import os
import six
import time

from typing import Dict, Any
from airflow import AirflowException
from airflow.configuration import conf
from airflow.utils.file import mkdirs

# TODO: Logging format and level should be configured
# in this file instead of from airflow.cfg. Currently
# there are other log format and level configurations in
# settings.py and cli.py. Please see AIRFLOW-1455.
LOG_LEVEL = conf.get('core', 'LOGGING_LEVEL').upper()
# logging_level = INFO

# Flask appbuilder's info level log is very verbose,
# so it's set to 'WARN' by default.
FAB_LOG_LEVEL = conf.get('core', 'FAB_LOGGING_LEVEL').upper()
# fab_logging_level = WARN

LOG_FORMAT = conf.get('core', 'LOG_FORMAT')
# log_format = [%%(asctime)s] [ %%(process)s - %%(name)s] {%%(filename)s:%%(lineno)d} %%(levelname)s - %%(message)s

COLORED_LOG_FORMAT = conf.get('core', 'COLORED_LOG_FORMAT')
# colored_log_format = [%%(blue)s%%(asctime)s%%(reset)s] {%%(blue)s%%(filename)s:%%(reset)s%%(lineno)d} %%(log_color)s%%(levelname)s%%(reset)s - %%(log_color)s%%(message)s%%(reset)s

COLORED_LOG = conf.getboolean('core', 'COLORED_CONSOLE_LOG')
# colored_console_log = True

COLORED_FORMATTER_CLASS = conf.get('core', 'COLORED_FORMATTER_CLASS')
# colored_formatter_class = airflow.utils.log.colored_log.CustomTTYColoredFormatter

BASE_LOG_FOLDER = conf.get('core', 'BASE_LOG_FOLDER')
# base_log_folder = /usr/local/airflow/logs

PROCESSOR_LOG_FOLDER = conf.get('scheduler', 'CHILD_PROCESS_LOG_DIRECTORY')
# child_process_log_directory = /usr/local/airflow/logs/scheduler

DAG_PROCESSOR_MANAGER_LOG_LOCATION = conf.get('core', 'DAG_PROCESSOR_MANAGER_LOG_LOCATION')
# dag_processor_manager_log_location = /usr/local/airflow/logs/dag_processor_manager/dag_processor_manager.log

FILENAME_TEMPLATE = conf.get('core', 'LOG_FILENAME_TEMPLATE')
# log_filename_template = {{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log

PROCESSOR_FILENAME_TEMPLATE = conf.get('core', 'LOG_PROCESSOR_FILENAME_TEMPLATE')
# log_processor_filename_template = {{ filename }}.log

FORMATTER_CLASS_KEY = '()' if six.PY2 else 'class'
# 파이썬 2와 3의 호환성을 고려해 로그 포맷터의 키를 결정한다.
# 파이썬 2에서는 ()를 사용하고, 파이썬 3에서는 class를 사용한다.

CUSTOM_TASK_FILENAME_TEMPLATE = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}_{{ log_level }}.log"
CUSTOM_PROCESSOR_FILENAME_TEMPLATE = "{{ filename }}_{{ log_level }}.log"

def create_task_handler(log_level):
    return {
        'class': 'airflow.utils.log.file_task_handler.FileTaskHandler',
        'level': log_level,
        'formatter': 'airflow',
        'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        'filename_template': CUSTOM_TASK_FILENAME_TEMPLATE.replace('{{ log_level }}', log_level.lower()),
    }
def create_process_handler(log_level):
    return {
        'class': 'airflow.utils.log.file_processor_handler.FileProcessorHandler',
        'level': log_level,
        'formatter': 'airflow',
        'base_log_folder': os.path.expanduser(PROCESSOR_LOG_FOLDER),
        'filename_template': CUSTOM_PROCESSOR_FILENAME_TEMPLATE.replace('{{ log_level }}', log_level.lower()),
    }

DEFAULT_LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'airflow': {
            'format': LOG_FORMAT
        },
        'airflow_coloured': {
            'format': COLORED_LOG_FORMAT if COLORED_LOG else LOG_FORMAT,
            FORMATTER_CLASS_KEY: COLORED_FORMATTER_CLASS if COLORED_LOG else 'logging.Formatter'
        },
    },
    'handlers': {
        'console': {
            'class': 'airflow.utils.log.logging_mixin.RedirectStdHandler',
            'formatter': 'airflow_coloured',
            'stream': 'sys.stdout'
        },
        'debug_task': create_task_handler('DEBUG'),
        'info_task': create_task_handler('INFO'),
        'warning_task': create_task_handler('WARNING'),
        'error_task': create_task_handler('ERROR'),
        'critical_task': create_task_handler('CRITICAL'),
        'info_processor': create_process_handler('INFO'),
        'warning_processor': create_process_handler('WARNING'),
        'error_processor': create_process_handler('ERROR')
    },
    # logger의 level은 사용한 핸들러의 최상위 level을 사용해야함
    'loggers': {
        'airflow.processor': {
            'handlers': ['info_processor', 'warning_processor', 'error_processor'],
            'level': 'INFO',
            'propagate': False,
        },
        'airflow.task': {
            'handlers': ['debug_task', 'info_task', 'warning_task', 'error_task', 'critical_task'],
            'level': 'DEBUG',
            'propagate': False,
        },
        'flask_appbuilder': {
            'handler': ['console'],
            'level': FAB_LOG_LEVEL,
            'propagate': True,
        }
    },
    'root': {
        'handlers': ['console'],
        'level': LOG_LEVEL,
    }
}  # type: Dict[str, Any]

DEFAULT_DAG_PARSING_LOGGING_CONFIG = {
    'handlers': {
        'processor_manager': {
            'class': 'logging.handlers.RotatingFileHandler',
            'formatter': 'airflow',
            'filename': DAG_PROCESSOR_MANAGER_LOG_LOCATION,
            'mode': 'a',
            'maxBytes': 104857600,  # 100MB
            'backupCount': 5
        }
    },
    'loggers': {
        'airflow.processor_manager': {
            'handlers': ['processor_manager'],
            'level': LOG_LEVEL,
            'propagate': False,
        }
    }
}

# Only update the handlers and loggers when CONFIG_PROCESSOR_MANAGER_LOGGER is set.
# This is to avoid exceptions when initializing RotatingFileHandler multiple times
# in multiple processes.
if os.environ.get('CONFIG_PROCESSOR_MANAGER_LOGGER') == 'True':
    DEFAULT_LOGGING_CONFIG['handlers'] \
        .update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'])
    DEFAULT_LOGGING_CONFIG['loggers'] \
        .update(DEFAULT_DAG_PARSING_LOGGING_CONFIG['loggers'])

    # Manually create log directory for processor_manager handler as RotatingFileHandler
    # will only create file but not the directory.
    processor_manager_handler_config = DEFAULT_DAG_PARSING_LOGGING_CONFIG['handlers'][
        'processor_manager']
    directory = os.path.dirname(processor_manager_handler_config['filename'])
    mkdirs(directory, 0o755)

##################
# Remote logging #
##################

REMOTE_LOGGING = conf.getboolean('core', 'remote_logging')
S3_FILENAME_TEMPLATE = "{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}_{{ log_level }}.log"

def create_s3_task_handler(log_level):
    return {
        'class': 'airflow.utils.log.s3_task_handler.S3TaskHandler',
        'formatter': 'airflow',
        'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
        's3_log_folder': REMOTE_BASE_LOG_FOLDER,
        'filename_template': S3_FILENAME_TEMPLATE.replace("{{ log_level }}", log_level.lower()),
    }

if REMOTE_LOGGING:

    ELASTICSEARCH_HOST = conf.get('elasticsearch', 'HOST')

    # Storage bucket URL for remote logging
    # S3 buckets should start with "s3://"
    # GCS buckets should start with "gs://"
    # WASB buckets should start with "wasb"
    # just to help Airflow select correct handler
    REMOTE_BASE_LOG_FOLDER = conf.get('core', 'REMOTE_BASE_LOG_FOLDER')

    if REMOTE_BASE_LOG_FOLDER.startswith('s3://'):
        S3_REMOTE_HANDLERS = {
            'debug_task': create_s3_task_handler('DEBUG'),
            'info_task': create_s3_task_handler('INFO'),
            'warning_task': create_s3_task_handler('WARING'),
            'error_task': create_s3_task_handler('ERROR'),
            'critical_task': create_s3_task_handler('CRITICAL')
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(S3_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('gs://'):
        GCS_REMOTE_HANDLERS = {
            'task': {
                'class': 'airflow.utils.log.gcs_task_handler.GCSTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
                'gcs_log_folder': REMOTE_BASE_LOG_FOLDER,
                'filename_template': FILENAME_TEMPLATE,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(GCS_REMOTE_HANDLERS)
    elif REMOTE_BASE_LOG_FOLDER.startswith('wasb'):
        WASB_REMOTE_HANDLERS = {
            'task': {
                'class': 'airflow.utils.log.wasb_task_handler.WasbTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
                'wasb_log_folder': REMOTE_BASE_LOG_FOLDER,
                'wasb_container': 'airflow-logs',
                'filename_template': FILENAME_TEMPLATE,
                'delete_local_copy': False,
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(WASB_REMOTE_HANDLERS)
    elif ELASTICSEARCH_HOST:
        ELASTICSEARCH_LOG_ID_TEMPLATE = conf.get('elasticsearch', 'LOG_ID_TEMPLATE')
        ELASTICSEARCH_END_OF_LOG_MARK = conf.get('elasticsearch', 'END_OF_LOG_MARK')
        ELASTICSEARCH_WRITE_STDOUT = conf.getboolean('elasticsearch', 'WRITE_STDOUT')
        ELASTICSEARCH_JSON_FORMAT = conf.getboolean('elasticsearch', 'JSON_FORMAT')
        ELASTICSEARCH_JSON_FIELDS = conf.get('elasticsearch', 'JSON_FIELDS')

        ELASTIC_REMOTE_HANDLERS = {
            'task': {
                'class': 'airflow.utils.log.es_task_handler.ElasticsearchTaskHandler',
                'formatter': 'airflow',
                'base_log_folder': os.path.expanduser(BASE_LOG_FOLDER),
                'log_id_template': ELASTICSEARCH_LOG_ID_TEMPLATE,
                'filename_template': FILENAME_TEMPLATE,
                'end_of_log_mark': ELASTICSEARCH_END_OF_LOG_MARK,
                'host': ELASTICSEARCH_HOST,
                'write_stdout': ELASTICSEARCH_WRITE_STDOUT,
                'json_format': ELASTICSEARCH_JSON_FORMAT,
                'json_fields': ELASTICSEARCH_JSON_FIELDS
            },
        }

        DEFAULT_LOGGING_CONFIG['handlers'].update(ELASTIC_REMOTE_HANDLERS)
    else:
        raise AirflowException(
            "Incorrect remote log configuration. Please check the configuration of option 'host' in "
            "section 'elasticsearch' if you are using Elasticsearch. In the other case, "
            "'remote_base_log_folder' option in 'core' section.")