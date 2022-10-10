import logging
import urllib.parse
from time import sleep
from datetime import datetime
import datetime

import requests,os,csv,backoff
from requests.auth import HTTPBasicAuth
from bizztreat_base.config import Config

logging.basicConfig(level=logging.INFO)


class SmartAdServerClient:

    def __init__(self, network_id, username, password):
        self.base_url = f"https://reporting.smartadserverapis.com/{network_id}/"
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(username, password)

    def _generate_report(self, report_config):
        url = urllib.parse.urljoin(self.base_url, f"reports")
        response = self.session.post(url, json=report_config)
        return response.json()["taskId"]

    @backoff.on_exception(backoff.expo, exception=TimeoutError,max_tries=3,)
    def _wait_till_reports_is_ready(self, task_id):
        url = urllib.parse.urljoin(self.base_url, f"reports/{task_id}")
        response = self.session.get(url)
        status = response.json()["lastTaskInstance"]["instanceStatus"]
        response_created_at = response.json()["creationDateUTC"]
        logging.info("Waiting for report to be generated, current status: %s", status)
        if status != "SUCCESS":
            sleep(5)
            if (datetime.datetime.now()-datetime.datetime.strptime(response_created_at,"%Y-%m-%dT%H:%M:%S")).total_seconds()/60 >= 130:
                logging.info(f"Report generating timed-out")
                raise TimeoutError()
            self._wait_till_reports_is_ready(task_id)


    def _get_reports_file(self, task_id):
        self._wait_till_reports_is_ready(task_id)
        url = urllib.parse.urljoin(self.base_url, f"reports/{task_id}/file")
        response = self.session.get(url)
        return response.content

    def write_csv(self,report_name,report_config,fname):
        logging.info(f"Generating report {report_name}")
        task_id = self._generate_report(report_config)
        get_data = self._get_reports_file(task_id).decode('ASCII')
        with open(fname,"w",newline="") as fid:
            output_writer = csv.writer(fid,dialect=csv.unix_dialect)
            for s in get_data.splitlines():
                output_writer.writerow(list(s.replace("\"","").replace(" ","").split(",")))


def main():
    proxy = Config()
    client = SmartAdServerClient(proxy["network_id"], proxy["username"], proxy["password"])
    for report_name, report_config in proxy["reports"].items():
        report_config["outputParameters"] = {
            "timeZone": "UTC",
            "displayHeader": True,
            "fieldDelimiter": "\"",
            "fieldSeparator": ", "
        }
    if not os.path.exists(proxy.output_folder):
        os.makedirs(proxy.output_folder)
    fname = os.path.join(proxy.output_folder,"smart.csv")
    client.write_csv(report_name,report_config,fname)

if __name__ == '__main__':
    main()
