from prometheus_client import start_http_server, Metric, REGISTRY
from prometheus_client.core import GaugeMetricFamily, CounterMetricFamily
import json
import requests
import sys
import time

class SparkAppsCollector(object):
  def __init__(self,sclient):
    self.sclient = sclient

  def collect(self):
    spark_apps = self.sclient.get_apps()
    spark_app_stats = self.sclient.parse_apps_stats(spark_apps)

    stage_metrics= {}
    for app_id in spark_app_stats.keys():
        print(app_id)
        spark_app_stages = self.sclient.get_stages(app_id)
        spark_app_stages_stats = self.sclient.parse_stages_stats(app_id, spark_app_stages)
        stage_metrics[app_id] = spark_app_stages_stats[app_id]

    executor_metrics = {}
    for app_id in spark_app_stats.keys():
      spark_app_executors = self.sclient.get_executors(app_id)
      spark_app_executors_stats = self.sclient.parse_executors_stats(app_id, spark_app_executors)
      executor_metrics[app_id] = spark_app_executors_stats[app_id]

    spark_app_executors_stats_final = self.sclient.merge_app_labels_to_stats(spark_app_stats,executor_metrics)
    yield from self.metrics_generator(spark_app_executors_stats_final)

    spark_app_stages_stats_final = self.sclient.merge_app_labels_to_stats(spark_app_stats,stage_metrics)
    yield from self.metrics_generator(spark_app_stages_stats_final)

    yield from self.app_metrics_generator(spark_app_stats)

  def app_metrics_generator(self,response):
    for app_id in response.keys():
        for m in response[app_id]["metrics"].keys():
          metric = Metric(m, m, 'gauge')
          metric.add_sample(m,value=response[app_id]["metrics"][m], labels=response[app_id]['labels'])
          yield metric

  def metrics_generator(self,response):
    for app_id in response.keys():
      for resource in response[app_id]["metrics"].keys():
        for m in response[app_id]["metrics"][resource].keys():
          metric = Metric(m, m, 'gauge')
          metric.add_sample(m,value=response[app_id]["metrics"][resource][m], labels=response[app_id]['labels'][resource])
          yield metric
