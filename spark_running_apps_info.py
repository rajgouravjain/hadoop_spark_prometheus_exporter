import logging
import fire
import requests
from datetime import datetime, timedelta
import pytz
import traceback


class SparkHelper(object):
    """Spark helper functions."""

    hadoop_rm_api_url = "ws/v1/cluster/apps"
    spark_history_api_url = "api/v1/applications"

    def __init__(self, rm, hs, hadoop_rm_api_port, spark_history_api_port, status="running"):
        self.rm = rm
        self.hs = hs
        self.spark_history_api_port = spark_history_api_port
        self.hadoop_rm_api_port = hadoop_rm_api_port
        self.status = status

    @staticmethod
    def create_uri(dn, port, url):
        """Create request uri."""

        return "http://%s:%s/%s" % (dn, port, url)

    def make_request(self, uri, params={}):
        """Make required request to spark cluster."""
        try:
            results = requests.get(uri, params=params)
            if results.status_code == 200:
                return results.json()
            else:
                results.raise_for_status()
        except Exception:
            print("Error while making request to {}, args {}".format(uri, params))
            print(traceback.format_exc())

    def get_apps(self, **params):
        """Get info for all applications based in params filters."""
        params = {"state": ",".join(self.status)}
        app_url = self.create_uri(self.rm, self.hadoop_rm_api_port, self.hadoop_rm_api_url)
        return self.make_request(app_url, params)

    def get_executors(self, app_id):
        """Get executor details for app."""
        try:
            print("Getting excecutor details for app {}".format(app_id))
            executor_uri = self.create_uri(self.hs, self.spark_history_api_port,
                                           self.spark_history_api_url + "/{}/allexecutors".format(app_id))
            return self.make_request(executor_uri)
        except requests.exceptions.HTTPError as req_err:
            print("Failed to get executor for app:{},error:{}".format(app_id, req_err))
            return []

    def get_stages(self, app_id):
        """Get stages of an app"""
        try:
            print("Getting stages details for app {}".format(app_id))
            stages_uri = self.create_uri(self.hs, self.spark_history_api_port,
                                         self.spark_history_api_url + "/{}/stages".format(app_id))
            return self.make_request(stages_uri)
        except requests.exceptions.HTTPError as req_err:
            print("Failed to get stages for app:{},error:{}".format(app_id, req_err))
            return []

    @staticmethod
    def parse_apps_stats(app_details):

        """Pick out the required fields from the raw spark details."""
        print("Parsing app stats")
        app_stats_dict = {}
        if app_details == None:
           return app_stats_dict
        if ("apps" in app_details) and (app_details["apps"] != None):
            if "app" in app_details["apps"]:
                for app in app_details["apps"]["app"]:
                    #start_time = timespamp_in_ms_to_utc(app["startedTime"])
                    #end_time = timespamp_in_ms_to_utc(app["finishedTime"])
                    start_time = app["startedTime"]
                    end_time  = app["finishedTime"]

                    app_id = app["id"]
                    app_stats_dict[app_id] = {}
                    app_stats_dict[app_id]["labels"] = {}
                    app_stats_dict[app_id]["metrics"] = {}

                    app_stats_dict[app_id]["labels"]["app_id"] = app["id"]
                    app_stats_dict[app_id]["labels"]["app_name"] = app["name"]
                    app_stats_dict[app_id]["labels"]["app_type"] = app["applicationType"]
                    app_stats_dict[app_id]["labels"]["app_tags"] = app["applicationTags"]
                    app_stats_dict[app_id]["labels"]["user"] = app["user"]
                    app_stats_dict[app_id]["labels"]["queue"] = app["queue"]

                    app_stats_dict[app_id]["metrics"]["hadoop_app_start_time"] = start_time
                    app_stats_dict[app_id]["metrics"]["hadoop_app_end_time"] = end_time
                    app_stats_dict[app_id]["metrics"]["spark_app_elapsed_time_seconds"] =  app["elapsedTime"] / float(1000)
                    app_stats_dict[app_id]["metrics"]["spark_app_memory_gb_hour"] = app["memorySeconds"] / float(1024 * 3600)
                    app_stats_dict[app_id]["metrics"]["spark_app_cpu_core_hour"] = app["vcoreSeconds"] / float(3600)
                    app_stats_dict[app_id]["metrics"]["spark_app_allocated_memory_gb"] = app["allocatedMB"]/float(1024)
                    app_stats_dict[app_id]["metrics"]["spark_app_allocated_cpu_core"] = app["allocatedVCores"]
                    app_stats_dict[app_id]["metrics"]["spark_app_reserved_memory_gb"] = app["reservedMB"] / float(1024)
                    app_stats_dict[app_id]["metrics"]["spark_app_reserved_cpu_core"] = app["reservedVCores"]
                    app_stats_dict[app_id]["metrics"]["spark_app_running_containers_count"] = app["runningContainers"]
                    app_stats_dict[app_id]["metrics"]["spark_app_queue_usage_percentage"] = app["queueUsagePercentage"]
                    app_stats_dict[app_id]["metrics"]["spark_app_cluster_usage_percentage"] = app["clusterUsagePercentage"]
                    app_stats_dict[app_id]["metrics"]["spark_app_preempted_memory_gb"] =  app["preemptedResourceMB"] / float(1024)
                    app_stats_dict[app_id]["metrics"]["spark_app_preempted_cpu_core"] = app["preemptedResourceVCores"]
                    app_stats_dict[app_id]["metrics"]["spark_app_preempted_memory_gb_hour"] = app["preemptedMemorySeconds"] / float(1024 * 3600)
                    app_stats_dict[app_id]["metrics"]["spark_app_preempted_cpu_core_hour"] = app["preemptedVcoreSeconds"] / float(3600)
                    if "resourceInfo" in app:
                        app_stats_dict[app_id]["metrics"]["spark_app_used_memory_gb"] = float(app["resourceInfo"]["resourceUsagesByPartition"][0]["used"]["memory"]) / float(1024)
                        app_stats_dict[app_id]["metrics"]["spark_app_used_cpu_core"] = app["resourceInfo"]["resourceUsagesByPartition"][0]["used"]["vCores"]
                        app_stats_dict[app_id]["metrics"]["spark_app_pending_memory_gb"] = app["resourceInfo"]["resourceUsagesByPartition"][0]["pending"][
                                                 "memory"] / float(1024)
                        app_stats_dict[app_id]["metrics"]["spark_app_pending_cpu_core"]= app["resourceInfo"]["resourceUsagesByPartition"][0]["pending"]["vCores"]

        return app_stats_dict

    @staticmethod
    def parse_executors_stats(app_id, details):
        print("parsing executor stats for app",app_id)
        spark_app_executors_dict = {}
        spark_app_executors_dict[app_id] = {}
        spark_app_executors_dict[app_id]["labels"] = {}
        spark_app_executors_dict[app_id]["metrics"] = {}
        if details == None:
           return spark_app_executors_dict
        for i in range(len(details)):
            executor_id = str(details[i]["id"])
            spark_app_executors_dict[app_id]["labels"][executor_id] = {}
            spark_app_executors_dict[app_id]["labels"][executor_id]["app_id"] = app_id
            spark_app_executors_dict[app_id]["labels"][executor_id]["spark_app_executor_id"] = executor_id
            spark_app_executors_dict[app_id]["labels"][executor_id]["spark_app_executor_active"] = str(details[i]["isActive"])
            spark_app_executors_dict[app_id]["labels"][executor_id]['spark_app_resource_type'] = "executor"

            spark_app_executors_dict[app_id]["metrics"][executor_id] = {}
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_num_active_tasks"] = float(details[i]["activeTasks"])
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_num_completed_tasks"] = float(
                details[i]["completedTasks"])
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_num_failed_tasks"] = float(details[i]["failedTasks"])
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_input_gb"] = float(
                details[i]["totalInputBytes"]) / float(1024 * 1024 * 1024)
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_shuffle_read_gb"] = float(
                details[i]["totalShuffleRead"]) / float(
                1024 * 1024 * 1024)
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_shuffle_write_gb"] = float(
                details[i]["totalShuffleWrite"]) / float(
                1024 * 1024 * 1024)
            spark_app_executors_dict[app_id]["metrics"][executor_id]["spark_app_execution_time_s"] = float(
                details[i]["totalDuration"]) / float(
                1000)

        return spark_app_executors_dict

    @staticmethod
    def parse_stages_stats(app_id,details):
        print("Parsing stages for app", app_id)
        spark_app_stages_dict = {}
        spark_app_stages_dict[app_id] ={}
        spark_app_stages_dict[app_id]["labels"] = {}
        spark_app_stages_dict[app_id]["metrics"] = {}
        if details == None:
            return spark_app_stages_dict
        for i in range(len(details)):
            #spark_app_stages_dict[app_id]["labels"]["app_id"] = app_id
            stage_id = str(details[i]["stageId"])

            spark_app_stages_dict[app_id]["labels"][stage_id] = {}
            spark_app_stages_dict[app_id]["labels"][stage_id]["spark_app_stage_id"] = stage_id
            spark_app_stages_dict[app_id]["labels"][stage_id]["spark_app_stage_status"] = details[i]["status"]
            spark_app_stages_dict[app_id]["labels"][stage_id]['spark_app_resource_type'] = "stage"

            spark_app_stages_dict[app_id]["metrics"][stage_id] = {}
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_num_active_tasks"] = float(details[i]["numActiveTasks"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_num_completed_tasks"] = float(details[i]["numCompleteTasks"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_num_failed_tasks"] = float(details[i]["numFailedTasks"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_input_gb"] = float(details[i]["inputBytes"]) / float(1024 * 1024 * 1024)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_input_records"] = float(details[i]["inputRecords"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_output_gb"] = float(details[i]["outputBytes"]) / float(1024 * 1024 * 1024)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_output_records"] = float(details[i]["outputRecords"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_shuffle_read_gb"] = float(details[i]["shuffleReadBytes"]) / float(
                1024 * 1024 * 1024)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_shuffle_read_records"] = float(details[i]["shuffleReadRecords"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_shuffle_write_gb"] = float(details[i]["shuffleWriteBytes"]) / float(
                1024 * 1024 * 1024)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_shuffle_write_records"] = float(details[i]["shuffleWriteRecords"])
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_execution_time_s"] = float(details[i]["executorRunTime"]) / float(
                1000)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_memory_spilled_mb"] = float(details[i]["memoryBytesSpilled"]) / float(
                1024 * 1024)
            spark_app_stages_dict[app_id]["metrics"][stage_id]["spark_app_disk_spilled_mb"] = float(details[i]["diskBytesSpilled"]) / float(
                1024 * 1024)


        return spark_app_stages_dict

    @staticmethod
    def merge_app_labels_to_stats(app_stats, stats):
        print("Merging app labels")
        merged_labels_dict = {}
        for app_id in app_stats.keys():
            src = app_stats[app_id]["labels"]
            dst = stats[app_id]["labels"]
            for s in dst.keys():
                resource_dst = dst[s]
                merged_labels_dict = {**src, **resource_dst}
                stats[app_id]["labels"][s] = merged_labels_dict
        return stats

if __name__ == '__main__':
    fire.Fire(SparkHelper)
