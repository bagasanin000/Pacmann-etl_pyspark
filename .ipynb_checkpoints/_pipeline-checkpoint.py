from _staging_pipeline import pipeline_staging
from _validation_pipeline import data_profiling
from _warehouse_pipeline import pipeline_dwh

if __name__ == "__main__":
    pipeline_staging()
    data_profiling()
    pipeline_dwh()