from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]


def test_all_dag_jobs_exist():
    dag = (ROOT / 'dags/chicago_taxi_pipeline.py').read_text()
    for name in ('extract_to_bronze', 'bronze_csv_to_parquet', 'dq1_bronze', 'write_silver_minio', 'dq2_silver', 'build_gold_minio', 'dq3_gold_minio', 'publish_silver_postgres', 'publish_star_schema', 'publish_gold_postgres', 'validate_bi_postgres'):
        assert f'task_id="{name}"' in dag


def test_all_modules_parse():
    for rel in ('silver/write_silver.py', 'data_quality/dq2_silver.py', 'gold/build_gold_minio.py', 'data_quality/dq3_gold_minio.py', 'gold/publish_bi.py'):
        ast.parse((ROOT / rel).read_text())


def test_medallion_paths_and_spark_gold():
    assert 'silver/chicago_taxi/' in (ROOT / 'silver/write_silver.py').read_text()
    assert 'gold/chicago_taxi/' in (ROOT / 'gold/build_gold_minio.py').read_text()
    assert 'gold_tables(silver)' in (ROOT / 'gold/build_gold_minio.py').read_text()
    assert 'silver/chicago_taxi/' in (ROOT / 'silver/load_silver.py').read_text()
