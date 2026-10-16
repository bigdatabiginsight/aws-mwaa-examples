#!/bin/sh
# MWAA startup script — runs on scheduler, worker, and webserver at component
# startup. Used here to expose the SNS topic ARN to the demo callbacks so
# deadline/monitor alerts are published to SNS (and land in email) instead of
# only being logged.
#
# Exported env vars set here are available to Airflow tasks, the Triggerer
# (async deadline callbacks), and workers (sync callbacks / the REST monitor).
export DEADLINE_ALERT_SNS_TOPIC_ARN="arn:aws:sns:<region>:<account-id>:<your-sns-topic>"
