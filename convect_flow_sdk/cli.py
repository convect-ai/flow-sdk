import click
import os
from .flow_app import list_app,FlowApp
from pprint import pprint
def run_command():
    @click.group()
    def cli_entry():
        pass

    cli_entry.add_command(regression_test)
    cli_entry()

@click.command(name="regression-test", help="regression test")
@click.option("--flow_host_url",
              type=click.STRING,
              required=False, help="flow host url")
@click.option("--flow_api_token",
              type=click.STRING,
              required=False, help="flow api token")
@click.option("--workspace_id",
                type=click.STRING,
              required=False, help="workspace id")
@click.option("--app_id",
                type=click.STRING,
              required=False, help="app id")
def regression_test(flow_host_url, flow_api_token, workspace_id,app_id):
    if flow_host_url is None:
        flow_host_url = os.getenv("FLOW_HOST")
    if flow_api_token is None:
        flow_api_token = os.getenv("FLOW_API_TOKEN")
    if workspace_id is None:
        workspace_id = os.getenv("FLOW_WORKSPACE_ID")
    if flow_host_url is None:
        raise ValueError("flow_host_url is not set")
    if flow_api_token is None:
        raise ValueError("flow_api_token is not set")
    if workspace_id is None:
        raise ValueError("workspace_id is not set")
    print("flow_host_url", flow_host_url)
    print("workspace_id", workspace_id)
    if app_id is not None:
        print("app_id", app_id)
        app = FlowApp(flow_host_url, flow_api_token, workspace_id, app_id)
        try:
            app.regression_test()
        except Exception as e:
            print("regression test failed for app_id ", app_id)
            print(e)
    else:
        # list all apps
        app_list = list_app(flow_host_url, flow_api_token)
        test_result = {}
        for app_id in app_list:
            app = FlowApp(flow_host_url, flow_api_token, workspace_id, app_id)
            _success = True
            try:
                app.regression_test()
                print(f"Regression test PASSED for app_id:{app_id}")
            except Exception as e:
                print(f"Regression test FAILED for app_id:{app_id}, with error: {e}")
                _success = False
            test_result[app_id] = _success
        print("regression test result:")
        pprint(test_result)


