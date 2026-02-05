from airflow.models import DagBag

dagbag = DagBag(dag_folder="dags", include_examples=False)

if dagbag.import_errors:
    raise Exception(f"DAG Import Errors: {dagbag.import_errors}")

print("DAG validation successful!")
