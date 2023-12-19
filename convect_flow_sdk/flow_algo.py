import json
import mimetypes
import os
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from pprint import pprint
import shutil
import requests
import tarfile
import zipfile
import hashlib
from constants import RunStatus

def extract_archive(archive_path, target_folder):
    """
    Extracts a .tar.gz, .tar, or .zip file to a target folder.

    Parameters:
    archive_path (str): The path to the archive file.
    target_folder (str): The path to the target folder where files will be extracted.

    Returns:
    None
    """
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    if tarfile.is_tarfile(archive_path):
        print("Extracting tar file")
        with tarfile.open(archive_path, "r:*") as tar:
            tar.extractall(path=target_folder)
    elif zipfile.is_zipfile(archive_path):
        print("Extracting zip file")
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(target_folder)
    else:
        raise ValueError(
            f"Unsupported file format for {archive_path}. Please provide a .tar.gz, .tar, or .zip file."
        )


def compress_to_tar_gz(source_folder, target_file):
    """
    Compresses the contents of a source folder into a .tar.gz file.

    Parameters:
    source_folder (str): The path to the source folder to be compressed.
    target_file (str): The path of the resulting .tar.gz file.

    Returns:
    None
    """
    # check if target_file parent folder exists
    target_folder = os.path.dirname(target_file)
    if not os.path.exists(target_folder):
        os.makedirs(target_folder)
    file_md5 = []
    with tarfile.open(target_file, "w:gz") as tar:
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                full_path = os.path.join(root, file)
                arcname = os.path.relpath(full_path, source_folder)
                tarinfo = tar.gettarinfo(full_path, arcname=arcname)
                with open(full_path, "rb") as fileobj:
                    tar.addfile(tarinfo, fileobj)
                    file_md5.append(hashlib.md5(fileobj.read()).hexdigest())
    # sort file_md5 to make sure the order is consistent
    file_md5 = file_md5.sort()
    return hashlib.md5(json.dumps(file_md5).encode()).hexdigest()

def generate_run_hash(
    flow_host, workspace_id, algo_id, run_command, config, input_data_md5
):
    import hashlib
    import json
    import os
    import uuid

    _data = {
        "flow_host": flow_host,
        "workspace_id": workspace_id,
        "algo_id": algo_id,
        "run_command": run_command,
        "config": json.dumps(config),
        "input_data_md5": input_data_md5,
    }
    return hashlib.sha256(json.dumps(_data).encode("utf-8")).hexdigest()

@dataclass
class FlowAlgo:
    flow_host_url: str = os.getenv("FLOW_HOST", None)
    flow_api_token: str = os.getenv("FLOW_API_TOKEN", None)
    flow_workspace_id: str = os.getenv("FLOW_WORKSPACE_ID", None)
    use_local_algo_cache: bool = True
    local_cache_dir: str = os.path.join(os.getcwd(), ".flow_algo_sdk_cache")

    def __post_init__(self):
        assert self.flow_host_url is not None, "FLOW_HOST is not set"
        assert self.flow_api_token is not None, "FLOW_API_TOKEN is not set"
        assert self.flow_workspace_id is not None, "FLOW_WORKSPACE_ID is not set"
        self.flow_host_url = self.flow_host_url.rstrip("/")
        if self.use_local_algo_cache:
            os.makedirs(self.local_cache_dir, exist_ok=True)


    @property
    def api_url(self):
        """
        Get flow api url
        :return:
        """
        return f"{self.flow_host_url}/flowopt-server/api/"

    def get_credential_header(self):
        """
        Get credential header
        :return:
        """
        if self.flow_api_token is None:
            raise ValueError("Flow api token is not set")
        return {"CAuthorization": f"bearer {self.flow_api_token}"}

    def list_algos(self):
        """
        List all algos in the workspace
        :return:
        a list of algo objects
        [{'active': True,
          'algo_id': '2f1a1fa9-2958-4afe-bb48-4239a960986d',
          'algo_manifest': {'algo_image': '932478379847.dkr.ecr.us-west-2.amazonaws.com/flow-algo-unilever-forecast:652d307',
                            'algo_manifest': {'algo_description': 'unknown',
                                              'algo_id': '2f1a1fa9-2958-4afe-bb48-4239a960986d',
                                              'algo_name': 'Unilever_Forecast',
                                              'algo_version': 'unknown',
                                              'run_commands': ['weekly_run']},
                            'driver_image': '932478379847.dkr.ecr.us-west-2.amazonaws.com/flowopt-commons:441d3bb@sha256:cdb981947837eae7977cb74c6cbb9706cda44d7609e63476f7ef4e8104ec06a1',
                            'kfp_experiment_id': '1f22618a-91cd-4c12-aa55-a8dddeaaebb7',
                            'kfp_experiment_name': 'algo#3fa85f64#2f1a1fa9-2958-4afe-bb48-4239a960986d',
                            'kfp_pipeline_id': '170e8950-d5a2-42aa-b38d-27aa3b69c6ee',
                            'kfp_pipeline_name': 'Unilever_Forecast#3fa85f64#2f1a1fa9-2958-4afe-bb48-4239a960986d',
                            'kfp_version_number': 'f610c216-6fe8-40d2-98a1-9f7f674008b6',
                            'workspace_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6'},
          'created_at': '2023-12-15T02:01:41.430610',
          'id': 'b22f3c35-4724-4398-91bf-7a9a58dede82',
          'manifest_updated_at': '2023-12-18T04:54:56.766627',
          'owner': 'bin@convect.ai',
          'updated_at': '2023-12-18T04:54:56.705156',
          'workspace_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6'}]
        """
        _api_url = f"{self.api_url}algos/list"
        pagination = {"page": 1, "page_size": 100}
        _data = {
            "workspace_id": self.flow_workspace_id,
        }
        r = requests.post(_api_url, headers=self.get_credential_header(), params=pagination)
        r.raise_for_status()
        return r.json()

    def clear_local_algo_cache(self):
        """
        Clear local algo cache, this will delete the local history of submitted runs
        :return:
        """
        # delete local cache
        if os.path.exists(self.local_cache_dir):
            try:
                shutil.rmtree(self.local_cache_dir)
            except PermissionError:
                print(f"Permission denied: Unable to delete {self.local_cache_dir}. Check your permissions.")
            except Exception as e:
                print(f"An error occurred: {e}")


    def list_algo_runs(self, algo_id, page=1, page_size=10):
        """
        List all algo runs for a given algo_id in the workspace
        :param algo_id: algo id
        :param page: page number
        :param page_size: page size
        :return: list of algo run objects
         [{'active': True,
          'algo_id': 'b22f3c35-4724-4398-91bf-7a9a58dede82',
          'command_parameters': {'cpu_request': '0',
                                 'memory_request': '0',
                                 'run_command': 'weekly_run',
                                 'run_config': '{"input_file": "input.csv", '
                                               '"output_file": "output.csv", '
                                               '"predict_start_week": "202348", '
                                               '"predict_end_week": "202348", "algo": '
                                               '"v03-percentile"}',
                                 's3_input_path': 's3://convect-data/flowopt-test/flowopt-server/app_file_upload//algo_run/fcaecb67-6a2c-45e2-8afe-3a001efb5abf/input.tar.gz',
                                 's3_output_path': 's3://convect-data/flowopt-test/flowopt-server/app_file_upload//algo_run/fcaecb67-6a2c-45e2-8afe-3a001efb5abf/output.tar.gz'},
          'created_at': '2023-12-18T21:36:14.862233',
          'id': 'fcaecb67-6a2c-45e2-8afe-3a001efb5abf',
          'owner': 'bin@convect.ai',
          'run_job_status': {'created_at': '2023-12-18 21:36:15+00:00',
                             'error': None,
                             'finished_at': '2023-12-18 21:37:00+00:00',
                             'status': 'Failed'},
          'updated_at': '2023-12-19T00:47:13.848726',
          'workspace_id': '3fa85f64-5717-4562-b3fc-2c963f66afa6'}]

        """
        _api_url = f"{self.api_url}algo_runs/list"
        pagination = {"page": page, "page_size": page_size}
        _data = {
            "workspace_id": self.flow_workspace_id,
            "algo_id": algo_id,
        }
        r = requests.post(_api_url, headers=self.get_credential_header(), params=pagination, json=_data)
        r.raise_for_status()
        return r.json()

    def check_status(self, run_id, timeout=7200, wait = True):
        """
        Check algo run status
        :param run_id: algo run id
        :return:
        """
        print(f"Checking algo run status for run_id: {run_id}")
        _api_url = f"{self.api_url}algo_runs/check"
        _data = {
            "run_id": run_id,
        }
        _run_completed = False
        _start_time = time.time()
        _end_time = _start_time + timeout
        run_status = RunStatus.UNKNOWN
        while time.time()< _end_time:
            try:
                r = requests.post(_api_url, headers=self.get_credential_header(), json=_data)
                r.raise_for_status()
                run_job_status = r.json()["run_job_status"].get("status", None)
                if run_job_status in ["Succeeded", "Failed", "Canceled"]:
                    _run_completed = True
                    if run_job_status == "Succeeded":
                        run_status = RunStatus.SUCCEEDED
                    elif run_job_status == "Failed":
                        run_status = RunStatus.FAILED
                    elif run_job_status == "Canceled":
                        run_status = RunStatus.CANCELLED
                    break
                run_status = RunStatus.RUNNING
                print(f"algo run {run_id} is still running, retrying in 3 seconds")
                time.sleep(3)
            except Exception as e:
                print(f"Failed to check algo run status: {e}, retrying in 10 seconds")
                time.sleep(10)
            if not wait:
                break
        if not _run_completed and wait:
            print(f"Timeout: algo run {run_id} did not complete in {timeout} seconds")
        return run_status


    def log(self,run_id):
        """
        Get algo run log
        :param run_id:
        :return:
        """
        status = self.check_status(run_id,wait=False)
        if status in [RunStatus.UNKNOWN, RunStatus.RUNNING]:
            print(f"algo run {run_id} is not completed, unable to get log")
            return None
        print(f"Getting algo run log for run_id: {run_id}, status: {status}")
        _api_url = f"{self.api_url}algo_runs/logs"
        r = requests.post(_api_url, headers=self.get_credential_header(), json={"run_id": run_id})
        try:
            r.raise_for_status()
        except Exception as e:
            print(f"Failed to get algo run log: {e}")
            return None
        res = r.json()
        # for nodes in res['nodes']:, only keep displayName=='flowopt-algo-run-process'
        run_node = None
        for l in res["nodes"]:
            if l["displayName"] == "flowopt-algo-run-process":
                run_node=l
                break
        if run_node is None:
            return None
        return run_node["main_log"]

    def submit(self, algo_id, command, config, input_path):
        """
        Submit an algo run
        :param algo_id: algo id
        :param command: run command
        :param config: run config dict or path to config file or json string
        :param input_path: input path for the run
        :return: run id
        """
        _api_url = f"{self.api_url}algo_runs/submit"
        if config is str:
            if os.path.exists(config):
                config = json.load(open(config, "r"))
            else:
                try:
                    config = json.loads(config)
                except Exception as e:
                    raise Exception(f"Failed to parse config: {e}")
        # check if input_path exists
        if not os.path.exists(input_path):
            raise Exception(f"{input_path} does not exist")
        # check if input_path is a folder
        if not os.path.isdir(input_path):
            raise Exception(f"{input_path} is not a folder")
        # create temp folder and tar zip all files in input_path to temp folder as input.tar.gz using python tarfile
        with tempfile.TemporaryDirectory() as temp_dir:
            input_tar_gz_file = os.path.join(temp_dir, "input.tar.gz")
            input_file_md5 = compress_to_tar_gz(input_path, input_tar_gz_file)
            run_hash = generate_run_hash(
                self.flow_host_url, self.flow_workspace_id, algo_id, command, config, input_file_md5
            )
            # check if run_hash exists
            _run_hash_file_name = os.path.join(self.local_cache_dir,f"algo-run-{run_hash}.json")
            if os.path.exists(_run_hash_file_name):
                # load run_hash_file
                _run_hash_file = json.load(open(_run_hash_file_name, "r"))
                run_id = _run_hash_file["run_id"]
                print(f"algo run submitted with run_id: {run_id}")
            else:
                with open(input_tar_gz_file, "rb") as f:
                    file_content = f.read()
                    files = {
                        "file": (os.path.basename(input_tar_gz_file), file_content),
                    }
                    _data = {
                        "algo_id": algo_id,
                        "workspace_id": self.flow_workspace_id,
                        "run_command": command,
                        "config": json.dumps(config),
                    }
                    r = requests.post(
                        _api_url,
                        headers=self.get_credential_header(),
                        data=_data,
                        files=files,
                    )
                r.raise_for_status()
                with open(_run_hash_file_name, "w") as f:
                    json.dump(r.json(), f, indent=4)
                run_id = r.json()["run_id"]
                print(f"algo run submitted with run_id: {run_id}")
            return run_id

    def gather(self, run_id, output_path):
        """
        Gather algo run results
        :param run_id: algo run id
        :param output_path: output path for the run
        :return:
        """
        # check run status
        status = self.check_status(run_id, wait=False)
        if status == RunStatus.UNKNOWN:
            print(f"algo run {run_id} is not completed, unable to gather results")
            return None
        if status == RunStatus.FAILED:
            print(f"algo run {run_id} failed, unable to gather results")
            return None
        if status == RunStatus.CANCELLED:
            print(f"algo run {run_id} canceled, unable to gather results")
            return None
        if status == RunStatus.RUNNING:
            print(f"algo run {run_id} is still running, unable to gather results")
            return None
        # status == RunStatus.SUCCEEDED
        _data = {"run_id": run_id, "file_type": "OUTPUT"}
        _api_url = f"{self.api_url}algo_runs/download"
        r = requests.post(
            _api_url,
            json=_data,
            headers=self.get_credential_header(),
        )
        r.raise_for_status()
        os.makedirs(output_path, exist_ok=True)
        # write file to temp file and extract to output_path
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file = os.path.join(temp_dir, "output.tar.gz")
            with open(temp_file, "wb") as f:
                f.write(r.content)
            extract_archive(temp_file, output_path)
        print("gather algo run successfully")

    def terminate(self, run_id):
        """
        Terminate an algo run
        :param run_id: algo run id
        :return:
        """
        _api_url = f"{self.api_url}algo_runs/terminate"
        _data = {"run_id": run_id}
        r = requests.post(
            _api_url,
            json=_data,
            headers=self.get_credential_header(),
        )
        try:
            r.raise_for_status()
        except Exception as e:
            print(f"Failed to terminate algo run: {e}")
            return None
        # delete local cache
        # go through all files in local_cache_dir, if run_id is in the loaded json file, delete the file
        for file in os.listdir(self.local_cache_dir):
            _file_path = os.path.join(self.local_cache_dir, file)
            if os.path.isfile(_file_path):
                _run_hash_file = json.load(open(_file_path, "r"))
                if _run_hash_file["run_id"] == run_id:
                    os.remove(_file_path)
                    break
        print("terminate algo run successfully")
