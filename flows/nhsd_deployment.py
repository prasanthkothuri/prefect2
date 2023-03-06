from prefect_shell import shell_run_command
from prefect import flow, task
from prefect_email import  email_send_message, EmailServerCredentials
import os
import pymysql
from datetime import datetime
from common.static_variables import *
import awswrangler as wr