# flow-sdk
flow sdk 

## Install

```bash
pip install convect-flow-sdk
```

## Usage

```bash
# set up environment variables
# FLOW_WORKSPACE_ID: your flow workspace id
# FLOW_API_TOKEN: your flow api token from the workspace
# FLOW_HOST: flow host, default to https://flow.convect.ai
from convect_flow_sdk import FlowAlgo
from pprint import pprint
# this is an example of how to use the flow_algo_sdk
flow_algo = FlowAlgo()
# this algo_id is will be the one defined in the algo project
algo_id = '2f1a1fa9-2958-4afe-bb48-4239a960986d'
pprint(flow_algo.list_algos())
pprint(flow_algo.list_algo_runs(algo_id))
# this submit will zip all the files in path_to_input folder and submit to flow
run_id =flow_algo.submit(algo_id,
                 "weekly_run",
                 {
                     "input_file": "input.csv",
                     "output_file": "output.csv",
                     "predict_start_week": "202348",
                     "predict_end_week": "202348",
                     "algo": "v03-percentile"
                 },
                 "path_to_input")
# flow_algo.terminate(run_id)
flow_algo.check_status(run_id)
flow_algo.log(run_id)
# this gather will download the output.tar.gz from flow and extract to path_to_output folder
flow_algo.gather(run_id,"./output")
# flow_algo_sdk.clear_local_algo_cache()
# clear local algo cache will delete the local history of submitted runs
```
