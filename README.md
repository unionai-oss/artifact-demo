# Artifact Service

The purpose of this repo is to gather feedback and circulate the initial UX for the artifact story we are building out.

This contains code that will need updated versions of flytekit, and the open-source flyte sandbox. If you have an existing sandbox, you will need to restart it.

NB: The artifact service itself stores some information (most importantly the Triggers) in memory only. They are not persisted to the local database yet as we are just gathering feedback on the UX. If you restart the sandbox, triggers will need to be re-registered to take effect. 

## Setup Instructions
### Flyte sandboxed environment
You will need the open-source `flytectl` utilty for this. Please see installation instructions on that [repo](https://github.com/flyteorg/flytectl#-quick-start).

If you already have a local sandbox running, first stop it with
```bash
flytectl demo teardown
```

```bash
flytectl demo start --image ghcr.io/flyteorg/flyte-sandbox:a_v0.1.0
```

### Python
Create a new virtual environment for this project and install dependencies

```bash
python3 -m venv ~/envs/artifacts
. ~/envs/artifacts/bin/activate
pip install jupyterlab
pip install -U --force-reinstall "git+https://github.com/flyteorg/flytekit.git@artifacts"
pip install --no-deps -U --force-reinstall "git+https://github.com/flyteorg/flyteidl.git@artifacts"
```

Find a suitable place in your source folder and git clone this repo.

```bash
git clone git@github.com:unionai-oss/artifact-demo.git
```

For those familiar with the regular `flytectl demo` environment, it should look and behave the same. On the backend there's basically just one more service (the artifact service) and one more dependency (redis, which is used as a local-only stand-in for pub/sub).

## Usage
There are two source files in here currently. There is a companion notebook `artifact_user.ipynb` also which walks through the proposed ux.

* `demo_ml.py` - This was the running example using the version of Flyte/Union we showed earlier. It introduces the concepts of Artifacts and shows how they might be useful.  
* `demo_timeseries.py` - This is meant to mimic a more traditional data engineering pipeline. The new construct introduced here is the idea of a Trigger.

To register these files, run

```bash
pyflyte -c ~/.flyte/config-sandbox.yaml register --image ghcr.io/flyteorg/flytecookbook:a_6ffaa3af865d082d511f9533a659f8e5bd6dfa6b artifact_ux
```

The latest image for this repo is `ghcr.io/flyteorg/flytecookbook:a_6ffaa3af865d082d511f9533a659f8e5bd6dfa6b`. If the requirements are changed, we will update the image here. If you want to add your own pip/apt requirements, you'll have to rebuild the image as well - just make sure you include the non-merged `flytekit` and `flyteidl` pypi packages.

Please look through the Jupyter notebook for a To enable access to the S3 bucket (hosted by Minio) from jupyter, run the notebook with the extra env vars below

```bash
FLYTE_AWS_ACCESS_KEY_ID=minio FLYTE_AWS_SECRET_ACCESS_KEY=miniostorage FLYTE_AWS_ENDPOINT=http://localhost:30002 jupyter notebook
```
