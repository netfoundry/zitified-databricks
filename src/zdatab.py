import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.catalog import VolumeType
import openziti
import traceback
import argparse
import mlflow

# --- Demo intent ---
# This script is NOT a Databricks tutorial.
# Its purpose is to demonstrate that Databricks SDK calls
# (volumes, DBFS uploads, MLflow experiments, jobs)
# execute transparently over an OpenZiti overlay.

# Workspace path for your user in Databricks
profile_name = "free-profile"

# Tell the SDK which Databricks profile to use
os.environ["DATABRICKS_CONFIG_PROFILE"] = profile_name

# Catalog/schema/volume names for Unity Catalog
catalog_name = "workspace"
schema_name = "default"
volume_name = "datafiles"

# Demo job name
job_name = "demo-job"

#volume path
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"

def main():
    parser = argparse.ArgumentParser(description="Zitified Databricks SDK Demo")
    parser.add_argument("--ziti-identity", type=str, required=True, help="The OpenZiti identity file")
    parser.add_argument("--csv-file", type=str, required=True, help="The csv file path to upload to the volume")
    args = parser.parse_args()
    # Load Ziti identity for secure overlay connectivity
    print("Loading the OpenZiti identity...")
    ztx, err = openziti.load(args.ziti_identity)

    try:
        # --- Ziti overlay starts here ---
        # Monkeypatch the Python runtime so ALL SDK calls below
        # are transparently tunneled through OpenZiti.
        with openziti.monkeypatch():
            # Initialize Databricks SDK client
            workspace_client = WorkspaceClient(profile=profile_name)
            current_user = workspace_client.current_user.me()
            user_path = f"/Users/{current_user.user_name}/"
            # Notebook path for the demo job
            notebook_path = f"{user_path}demo_notebook"

            # --- Volume existence check ---
            # This is a normal SDK call, but the traffic flows through Ziti.
            volume_exists = False
            volumes = workspace_client.volumes.list(catalog_name=catalog_name, schema_name=schema_name)
            if volumes:
                for vol in volumes:
                    if vol.name == volume_name:
                        volume_exists = True

            # Create the volume if it doesn’t exist
            if not volume_exists:
                workspace_client.volumes.create(
                    catalog_name=catalog_name,
                    schema_name=schema_name,
                    name=volume_name,
                    volume_type=VolumeType.MANAGED,  # Managed volume (Databricks controls lifecycle)
                    storage_location=None,           # None for managed volumes
                    comment="Volume for CSV uploads"
                )
                print(f"Created new volume: {volume_path}")
            else:
                print(f"Volume {volume_name} already exists")

            # Print info about volumes in the schema
            volumes = workspace_client.volumes.list(catalog_name=catalog_name, schema_name=schema_name)
            if volumes:
                for vol in volumes:
                    print("Volume info:", vol.name, vol.volume_type, vol.storage_location)

            # --- Upload file into the volume ---
            # Again, a standard SDK call — transparently tunneled via Ziti.
            with open(args.csv_file, "rb") as f:
                print(f"Uploading {f.name} to volume at {volume_path}...")
                workspace_client.dbfs.upload(f"{volume_path}{os.path.basename(f.name)}", f, overwrite=True)
            # --- MLflow experiment setup ---
            # MLflow calls also succeed over the Ziti overlay.
            mlflow.set_tracking_uri("databricks://" + profile_name)
            experiment_name = user_path + "demo_experiment"
            experiment = mlflow.get_experiment_by_name(experiment_name)

            if not experiment:
                experiment_id = mlflow.create_experiment(name=experiment_name)
                print(f"Created new experiment: ID={experiment_id}")
            else:
                print(f"Experiment already exists: ID={experiment.experiment_id}")

            # --- Job existence check ---
            # Jobs API calls are routed through Ziti without modification.
            job_exists = False
            job_list = list(workspace_client.jobs.list())
            for job in job_list:
                if job.settings.name == job_name:
                    job_exists = True

            # Create job if missing
            if not job_exists:
                job = workspace_client.jobs.create(
                    name=job_name,
                    tasks=[
                        jobs.Task(
                            task_key="task1",
                            notebook_task=jobs.NotebookTask(
                                notebook_path=notebook_path  # Notebook path in workspace
                            )
                        )
                    ]
                )
                print(f"Created Job: name={job_name}")
            else:
                print(f"Job already exists: name={job_name}")

        # --- End of Ziti overlay ---
        # All calls above were tunneled transparently through OpenZiti.

        # --- List jobs after overlay block ---
        if workspace_client:
            print("List jobs:")
            job_list = list(workspace_client.jobs.list())
            print("Number of jobs:", len(job_list))
            for job in job_list:
                print(job.settings.name)

    except Exception as e:
        print("Error:", e)
        traceback.print_exc()

if __name__ == "__main__":
    main()
