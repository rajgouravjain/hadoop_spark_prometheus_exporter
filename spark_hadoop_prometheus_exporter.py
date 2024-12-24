from prometheus_client import start_http_server, Metric, REGISTRY
import fire
import json
import requests
import sys
import time
from spark_running_apps_info import SparkHelper
from spark_apps_collector import SparkAppsCollector

class SparkExporter(object):
  def __init__(self,prom_port,metric_url="/metrics"):
    self.prom_port= prom_port
    self.metric_url = metric_url

  def run(self,rm,hs,hadoop_rm_api_port,spark_history_api_port,status=("RUNNING","RUNNING")):
    sclient = SparkHelper(rm=rm, hs=hs, hadoop_rm_api_port=hadoop_rm_api_port, spark_history_api_port=spark_history_api_port,status=status)
    start_http_server(int(self.prom_port))
    REGISTRY.register(SparkAppsCollector(sclient=sclient))
    try:
      while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

    self.shutdown()

  def shutdown(self):
    sys.exit(0)

if __name__ == '__main__':
  fire.Fire(SparkExporter)
