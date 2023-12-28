import json
import mimetypes
import os
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pprint import pprint
import requests
import re
import uuid
from .constants import DataType, LangType, RunStatus


def extract_error_message(log_string):
    combined_pattern = re.compile(
        r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ERROR.*?(?=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|$)|Traceback \(most recent call last\):[\s\S]+?(?=\d{4}-\d{2}-\d{2} |\Z))",
        re.DOTALL,
    )
    # pattern = re.compile(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} ERROR.*?(?=\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}|$))", re.DOTALL)
    matches = combined_pattern.findall(log_string)
    return "\n\n".join(matches)


def extract_exception(log: str) -> str:
    pattern = r"(Traceback \(most recent call last\):[\s\S]+?(?=\d{4}-\d{2}-\d{2} |\Z))"
    matches = re.findall(pattern, log)
    traceback = "\n".join(matches)
    return traceback


def list_app(flow_host_url=None,flow_api_token=None):
    """
    List all apps in the workspace
    :return:
    """
    print("Listing all apps accessible by the api token...")
    if flow_host_url is None:
        flow_host_url = os.getenv("FLOW_HOST", None)
    if flow_api_token is None:
        flow_api_token = os.getenv("FLOW_API_TOKEN", None)
    if flow_host_url is None:
        raise ValueError("Flow host url is not set")
    if flow_api_token is None:
        raise ValueError("Flow api token is not set")
    flow_host_url = flow_host_url.rstrip("/")
    api_url = f"{flow_host_url}/flowopt-server/api/apps/list"
    r = requests.get(api_url, headers={"CAuthorization": f"bearer {flow_api_token}"}, params={"page": 1, "page_size": 99})
    r.raise_for_status()
    app_list = r.json()
    for app in app_list:
        print(app["id"],app["app_manifest"]["display_name"]["zh"])
    return [app["id"] for app in app_list]

@dataclass
class FlowApp:
    flow_host_url: str = os.getenv("FLOW_HOST", None)
    flow_api_token: str = os.getenv("FLOW_API_TOKEN", None)
    flow_workspace_id: str = os.getenv("FLOW_WORKSPACE_ID", None)
    flow_app_id: str = None

    def __post_init__(self):
        if self.flow_host_url is None:
            raise ValueError("Flow host url is not set")
        if self.flow_api_token is None:
            raise ValueError("Flow api token is not set")
        if self.flow_workspace_id is None:
            raise ValueError("Flow workspace id is not set")
        if self.flow_app_id is None:
            raise ValueError("Flow app id is not set")
        self.flow_host_url = self.flow_host_url.rstrip("/")

    @property
    def api_url(self):
        return f"{self.flow_host_url}/flowopt-server/api/"

    def get_credential_header(self):
        if self.flow_api_token is None:
            raise ValueError("Flow api token is not set")
        return {"CAuthorization": f"bearer {self.flow_api_token}"}

    def get_workspace_id(self):
        if self.flow_workspace_id is None:
            raise ValueError("Flow workspace id is not set")
        return self.flow_workspace_id

    def get_app_id(self):
        if self.flow_app_id is None:
            raise ValueError("Flow app id is not set")
        return self.flow_app_id

    def get_locked_instance_list(self):
        """
        Get locked instance list for the given workspace and app
        When user locked the instance in Flow platform, the instance can be retrieved by this function
        :param workspace_id:
        :param app_id:
        :return:
        """
        _url = self.api_url + "run_instances/list"
        _data = {
            "workspace_id": self.get_workspace_id(),
            "is_locked": True,
            "app_id": self.get_app_id(),
            "order_by_locked_at": "desc",
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        res = [
            {
                "instance_id": i["id"],
                "name": i["name"],
                "description": i["description"],
                "locked_at": i["locked_at"],
            }
            for i in r.json()
        ]
        return res

    def check_flow_connection(self):
        try:
            self.get_locked_instance_list()
        except Exception as e:
            return False
        return True

    def get_app_data(self):
        app_id = self.get_app_id()
        _url = self.api_url + f"workspace/{self.flow_workspace_id}/all_apps"
        # r = request_cache(_url, headers_str=json.dumps(self.get_credential_header()))
        r = requests.get(_url, headers=self.get_credential_header(), params={"page": 1, "page_size": 99})
        r.raise_for_status()
        app_list = r.json()
        for app in app_list:
            if app["id"] == app_id:
                return app
        raise ValueError(f"App id {app_id} not found")

    def get_app_endpoint(self):
        app = self.get_app_data()
        return app["app_manifest"]["endpoint"]

    def download_instance_data(
        self, instance_id, data_type: DataType, language: LangType, out_path: str
    ):
        # download the input or output view data (same as the ui view data)
        app_id = self.get_app_id()
        app_endpoint = self.get_app_endpoint()
        _url = self.flow_host_url + "/" + app_endpoint + f"/api/data/{instance_id}"
        _data = {"data_type": data_type.value, "lang": language.value}
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)

    def download_instance_raw_data(self, instance_id, out_path: str):
        # download the instance raw input data (based on the input data model in the database)
        # the sheet and column name are based on the input data model, and can be re-use to create new instance by using raw_import process
        app_endpoint = self.get_app_endpoint()
        _url = self.flow_host_url + "/" + app_endpoint + f"/api/raw_data/{instance_id}"
        r = requests.get(_url, headers=self.get_credential_header())
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)

    def download_instance_user_input_data(self, instance_id, out_path: str):
        # download the user uploaded input data (the user initial uploaded data)
        _url = self.api_url + "tasks/download_file"
        _data = {"run_instance_id": instance_id}
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)

    def create_folder(self, name, description=""):
        workspace_id = self.get_workspace_id()
        app_id = self.get_app_id()
        _url = self.api_url + "sessions/create"
        _data = {
            "workspace_id": workspace_id,
            "app_id": app_id,
            "name": name,
            "description": description,
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        # print(r.json())
        # return folder id
        return r.json()["id"]

    def get_folders(self, active=True,page=1,page_size=99):
        workspace_id = self.get_workspace_id()
        app_id = self.get_app_id()
        _url = self.api_url + "sessions/list"
        _data = {
            "workspace_id": workspace_id,
            "app_id": app_id,
            "active": active,
            "order_by": "created_at",
            "order_by_created_at": "desc",
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data,
                          params={"page": 1, "page_size": 99})
        r.raise_for_status()
        res = [
            {
                "folder_id": i["id"],
                "name": i["name"],
                "description": i["description"],
                "created_at": i["created_at"],
            }
            for i in r.json()
        ]
        # print(res)
        return res

    def get_folder_details(self, folder_id):
        _url = self.api_url + f"sessions/get/{folder_id}"
        r = requests.get(_url, headers=self.get_credential_header())
        r.raise_for_status()
        # print(r.json())
        return r.json()

    def get_instances(self, folder_id, active=True,page=1,page_size=99):
        app_id = self.get_app_id()
        workspace_id = self.get_workspace_id()
        _url = self.api_url + "run_instances/list"
        _data = {
            "session_id": folder_id,
            "order_by_created_at": "desc",
            "active": active,
            "order_by": "created_at",
            "workspace_id": workspace_id,
            "app_id": app_id,
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data,
                          params={"page": 1, "page_size": 99})
        r.raise_for_status()
        # print(r.json())
        return r.json()

    def get_instance_details(self, instance_id):
        """
        Get instance details by instance id
        """
        _url = self.api_url + f"run_instances/get/{instance_id}"
        r = requests.get(_url, headers=self.get_credential_header())
        r.raise_for_status()
        # print(r.json())
        return r.json()

    def create_instance(
        self, name, file_path, folder_id, description="", raw_import=False
    ):
        # check if file_path exists
        if os.path.isfile(file_path) is False:
            raise FileNotFoundError(f"File {file_path} not found")
        _url = self.api_url + "tasks/upload_file"
        with open(file_path, "rb") as f:
            file_content = f.read()
            files = {
                "file": (os.path.basename(file_path), file_content),
            }
            r = requests.post(_url, headers=self.get_credential_header(), files=files)
        r.raise_for_status()
        # print(r.json())
        path = r.json()["path"]
        # print(path)
        _url = self.api_url + "tasks/import"
        _data = {
            "session_id": folder_id,
            "name": name,
            "description": description,
            "pipeline_name": "import_excel" if raw_import is False else "raw_import",
            "pipeline_config": {"config": {"file_path": path}},
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        # print(r.json())
        # return instance id
        return r.json()["id"]

    def clone_instance(self, source_instance_id, folder_id, name, description):
        _url = self.api_url + "tasks/clone"
        _data = {
            "active": True,
            "session_id": folder_id,
            "name": name,
            "description": description,
            "source_run_instance_id": source_instance_id,
            "pipeline_name": "raw_clone",
            "pipeline_config": {"config": {}},
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        # print(r.json())
        # return instance id
        return r.json()["id"]

    def re_import_instance(self, instance_id, file_path, raw_import=False):
        """
        Re-import instance data
        :param instance_id:
        :param file_path:
        :return:
        """
        # check if file_path exists
        if os.path.isfile(file_path) is False:
            raise FileNotFoundError(f"File {file_path} not found")
        _url = self.api_url + "tasks/upload_file"
        with open(file_path, "rb") as f:
            file_content = f.read()
            files = {
                "file": (os.path.basename(file_path), file_content),
            }
            r = requests.post(_url, headers=self.get_credential_header(), files=files)
        r.raise_for_status()
        # print(r.json())
        path = r.json()["path"]
        _url = self.api_url + "tasks/reimport"
        _data = {
            "run_instance_id": instance_id,
            "pipeline_name": "import_excel" if raw_import is False else "raw_import",
            "pipeline_config": {"config": {"file_path": path}},
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        # print(r.json())
        # return instance id
        return r.json()["id"]

    def update_instance_info(self, instance_id, name: str, description: str):
        _url = self.api_url + f"run_instances/update/{instance_id}"
        _data = {}
        if name is not None and name.strip() != "":
            _data["name"] = name
        if description is not None and description.strip() != "":
            _data["description"] = description
        if len(_data) == 0:
            return
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        return r.json()

    def solve_instance(self, folder_id, instance_id):
        _url = self.api_url + "tasks/solve"
        _data = {
            "session_id": folder_id,
            "run_instance_id": instance_id,
            "pipeline_name": "flowopt_solve",
            "pipeline_config": {
                "pre_solve_config": {},
                "post_solve_config": {},
            },
        }
        try:
            r = requests.post(_url, headers=self.get_credential_header(), json=_data)
            r.raise_for_status()
        except Exception as e:
            # try again with different pipeline config
            # right now we only have two types of pipeline config so we can do this
            # we need to parse the app manifest to get the pipeline config in the future
            _data = _data = {
            "session_id": folder_id,
            "run_instance_id": instance_id,
            "pipeline_name": "flowopt_solve",
            "pipeline_config": {
                "config": {},
            },
            }
            r = requests.post(_url, headers=self.get_credential_header(), json=_data)
            r.raise_for_status()

        # print(r.json())
        return r.json()["id"]

    def solve_all_instances(self, folder_id):
        instances = self.get_instances(folder_id)
        for instance in instances:
            # check if instance is ready
            readiness_status = instance["readiness_status"]
            # continue if not ready or solve process is triggered
            if readiness_status is None or readiness_status["status"] != "Succeeded":
                continue
            if instance["solve_process_id"] is not None:
                continue
            self.solve_instance(folder_id, instance["id"])
            print(f"Solve instance {instance['id']}")

    def get_logs(self, process_id):
        _url = self.api_url + f"tasks/logs"
        _data = {
            "process_id": process_id,
        }
        r = requests.post(_url, headers=self.get_credential_header(), json=_data)
        r.raise_for_status()
        # print(r.json())
        return r.json()

    def _get_status(self,status):
        if status is None:
            return RunStatus.UNKNOWN
        else:
            if status["status"] == "Succeeded":
                return RunStatus.SUCCEEDED
            elif status["status"] == "Failed":
                return RunStatus.FAILED
            elif status["status"] == "Cancelled":
                return RunStatus.CANCELLED
            else:
                return RunStatus.RUNNING

    def get_status(self, process_id):
        logs = self.get_logs(process_id)
        status = logs["process"]["status"]
        return self._get_status(status)

    def filter_error_logs(self, logs, only_error=True):
        errors = None
        nodes = logs["nodes"]
        for node in nodes:
            if node["phase"] != "Succeeded":
                errors = node["main_log"]
                if only_error:
                    errors = extract_error_message(errors)
                return errors
        return errors

    def get_import_logs(self, instance_id):
        import_process_id = self.get_import_process_id(instance_id)
        if import_process_id is not None:
            return self.get_logs(import_process_id)
        else:
            return None

    def get_solve_logs(self, instance_id):
        solve_process_id = self.get_solve_process_id(instance_id)
        if solve_process_id is not None:
            return self.get_logs(solve_process_id)
        else:
            return None

    def get_import_process_id(self, instance_id):
        try:
            instance = self.get_instance_details(instance_id)
            return instance["import_process_id"]
        except Exception as e:
            raise e

    def get_clone_process_id(self, instance_id):
        try:
            instance = self.get_instance_details(instance_id)
            return instance["clone_process_id"]
        except Exception as e:
            raise e

    def get_solve_process_id(self, instance_id):
        try:
            instance = self.get_instance_details(instance_id)
            return instance["solve_process_id"]
        except Exception as e:
            raise e

    def _get_readiness_status(self, instance_id):
        instance = self.get_instance_details(instance_id)
        readiness_status = instance["readiness_status"]
        return self._get_status(readiness_status)

    def get_readiness_status(
        self, instance_id, continue_checks=False, max_checks=30, sleep_time=2
    ):
        """
        Check if instance is ready
        :param instance_id:
        :param continue_checks:
        :param max_checks:
        :param sleep_time:
        :return:
        """
        readiness_status = RunStatus.UNKNOWN
        for i in range(max_checks):
            readiness_status = self._get_readiness_status(instance_id)
            if readiness_status == RunStatus.UNKNOWN:
                return RunStatus.UNKNOWN
            if readiness_status == RunStatus.SUCCEEDED:
                return readiness_status
            elif readiness_status == RunStatus.FAILED:
                return readiness_status
            elif readiness_status == RunStatus.CANCELLED:
                return readiness_status
            else:
                if continue_checks is True:
                    time.sleep(sleep_time)
                    continue
                else:
                    return readiness_status
        return readiness_status

    def _get_solve_status(self, instance_id):
        instance = self.get_instance_details(instance_id)
        solve_status = instance["solving_status"]
        return self._get_status(solve_status)

    def get_solve_status(
        self, instance_id, continue_checks=False, max_checks=30, sleep_time=2
    ):
        """
        Check if instance is ready
        :param instance_id:
        :param continue_checks:
        :param max_checks:
        :param sleep_time:
        :return:
        """
        solve_status = RunStatus.UNKNOWN
        for i in range(max_checks):
            solve_status = self._get_solve_status(instance_id)
            if solve_status == RunStatus.UNKNOWN:
                return RunStatus.UNKNOWN
            if solve_status == RunStatus.SUCCEEDED:
                return solve_status
            elif solve_status == RunStatus.FAILED:
                return solve_status
            elif solve_status == RunStatus.CANCELLED:
                return solve_status
            else:
                if continue_checks is True:
                    time.sleep(sleep_time)
                    continue
                else:
                    return solve_status
        return solve_status

    def get_app_name(self):
        app = self.get_app_data()
        return app["app_manifest"]["display_name"]["zh"]

    def get_app_help_doc_link(self):
        return f"{self.flow_host_url}/{self.get_app_endpoint()}/docs/"
        # return "https://flow.convect.ai/flowopt-app-flow-direction-llm/docs/"

    def get_app_link(self):
        return f"{self.flow_host_url}/app/{self.get_app_id()}/workbench/active"
        # return f"https://flow.convect.ai/app/{self.get_app_id()}/workbench/active"

    def get_app_input_template_link(
        self,
    ):
        # return "https://flow.convect.ai/flowopt-app-flow-direction-llm/docs/assets/data/input_sample_flow_direction_llm.xlsx"
        app = self.get_app_data()
        sample_file_url = None
        for pipeline in app["app_manifest"]["pipelines"]["IMPORT"]:
            if pipeline["name"] == "import_excel":
                sample_file_url = pipeline["sample_file_url"]
                break
        if sample_file_url is None:
            raise ValueError("Sample file url not found")
        return f"{self.get_app_help_doc_link()}{sample_file_url}"

    def get_folder_link(self, folder_id):
        # return f"https://flow.convect.ai/app/{self.get_app_id()}/session/{folder_id}/view/active"
        return f"{self.flow_host_url}/app/{self.get_app_id()}/session/{folder_id}/view/active"

    def get_instance_link(self, instance_id):
        # https://flow.convect.ai/app/8ae8ad94-cbbe-45d4-a579-cf0daf9ce80a/instance/191345bd-fda9-4772-a566-72c68cc01d06/view/active
        return f"{self.flow_host_url}/app/{self.get_app_id()}/instance/{instance_id}/view/active"

    def get_process_log_link(self, process_id):
        # https://flow.convect.ai/app/8ae8ad94-cbbe-45d4-a579-cf0daf9ce80a/executions/af19980d-b860-4698-aa70-c431cd9ef3af/logs
        return (
            f"{self.flow_host_url}/app/{self.get_app_id()}/executions/{process_id}/logs"
        )

    def regression_test(self):
        print("-" * 100)
        print("Do regression test for app:{}...".format(self.get_app_id()))
        print("-" * 100)
        # check if flow connection is ok
        test_id = f"{uuid.uuid4()}"
        print("Checking flow connection...")
        if self.check_flow_connection() is False:
            raise ValueError("Flow connection failed")
        print("Flow connection ok")
        # get app data
        print("Getting app data...")
        try:
            app_data = self.get_app_data()
        except Exception as e:
            raise ValueError("Get app data failed, ex:{}".format(e))
        # print("App data:")
        # pprint(app_data)
        # check app help doc link
        print("Checking app help doc link...")
        app_help_doc_link = self.get_app_help_doc_link()
        print(app_help_doc_link)
        # check if app help doc link is valid
        try:
            r = requests.get(app_help_doc_link)
            r.raise_for_status()
        except Exception as e:
            raise ValueError("App help doc link is not valid, ex:{}".format(e))
        print("App help doc link ok")
        # check app input template link
        print("Checking app input template link...")
        app_input_template_link = self.get_app_input_template_link()
        print(app_input_template_link)
        # check if app input template link is valid
        try:
            r = requests.get(app_input_template_link)
            r.raise_for_status()
        except Exception as e:
            raise ValueError("App input template link is not valid, ex:{}".format(e))
        print("App input template link ok")
        # download app input template
        print("Downloading app input template...")
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_file_path = os.path.join(tmp_dir, "input_template.xlsx")
            # write r.content to tmp_file_path
            with open(tmp_file_path, "wb") as f:
                f.write(r.content)
            print("App input template downloaded to {}".format(tmp_file_path))
            # create folder
            print("Creating folder...")
            try:
                folder_id = self.create_folder("regresion_test#{}".format(test_id))
            except Exception as e:
                raise ValueError("Create folder failed, ex:{}".format(e))
            print("Folder created, id:{}".format(folder_id))
            # get folder details
            print("Getting folder details...")
            try:
                folder_details = self.get_folder_details(folder_id)
            except Exception as e:
                raise ValueError("Get folder details failed, ex:{}".format(e))
            print("Folder details:")
            # pprint(folder_details)
            # upload input template
            print("Uploading input template...")
            try:
                instance_id = self.create_instance(
                    "test_template_file",
                    tmp_file_path,
                    folder_id,
                    raw_import=False,
                )
            except Exception as e:
                raise ValueError("Upload input template failed, ex:{}".format(e))
            print("Input template uploaded, instance id:{}".format(instance_id))
            # get instance details
            print("Getting instance details...")
            try:
                instance_details = self.get_instance_details(instance_id)
            except Exception as e:
                raise ValueError("Get instance details failed, ex:{}".format(e))
            print("Instance details:")
            # pprint(instance_details)
            # check if instance is ready
            print("Checking if instance is ready...")
            try:
                readiness_status = self.get_readiness_status(instance_id, True, 600, 2)
            except Exception as e:
                raise ValueError("Check if instance is ready failed, ex:{}".format(e))
            if readiness_status != RunStatus.SUCCEEDED:
                raise ValueError("Instance is not ready, status:{}".format(readiness_status))
            print("Instance is ready")
            # solve instance
            print("Solving instance...")
            try:
                solve_process_id = self.solve_instance(folder_id, instance_id)
            except Exception as e:
                raise ValueError("Solve instance failed, ex:{}".format(e))
            print("Instance solving, process id:{}".format(solve_process_id))
            # check if instance is solved
            print("Checking if instance is solved...")
            try:
                solve_status = self.get_solve_status(instance_id, True, 600, 2)
            except Exception as e:
                raise ValueError("Check if instance is solved failed, ex:{}".format(e))
            if solve_status != RunStatus.SUCCEEDED:
                raise ValueError("Instance is not solved, status:{}".format(solve_status))
            print("Instance is solved")
            # download instance output data
            print("Downloading instance output data...")
            try:
                tmp_file_path = os.path.join(tmp_dir, "output_data.xlsx")
                self.download_instance_data(
                    instance_id, DataType.OUTPUT, LangType.ZH, tmp_file_path
                )
                print("Instance output data downloaded to {}".format(tmp_file_path))
            except Exception as e:
                raise ValueError("Download instance output data failed, ex:{}".format(e))
            # check clone instance
            print("Checking clone instance...")
            try:
                clone_instance_id = self.clone_instance(
                    instance_id, folder_id, "test_clone", "test_clone"
                )
            except Exception as e:
                raise ValueError("Clone instance failed, ex:{}".format(e))
            print("Clone instance created, id:{}".format(clone_instance_id))
            # get clone instance details
            print("Getting clone instance details...")
            try:
                clone_instance_details = self.get_instance_details(clone_instance_id)
            except Exception as e:
                raise ValueError("Get clone instance details failed, ex:{}".format(e))
            print("Clone instance details:")
            # pprint(clone_instance_details)
            # check if clone instance is ready
            print("Checking if clone instance is ready...")
            try:
                clone_readiness_status = self.get_readiness_status(
                    clone_instance_id, True, 600, 2
                )
            except Exception as e:
                raise ValueError(
                    "Check if clone instance is ready failed, ex:{}".format(e)
                )
            if clone_readiness_status != RunStatus.SUCCEEDED:
                raise ValueError(
                    "Clone instance is not ready, status:{}".format(
                        clone_readiness_status
                    )
                )
            print("Clone instance is ready")



