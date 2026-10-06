import importlib.util
import sys
from pathlib import Path
from types import ModuleType


def test_create_spark_session_requires_glue_context() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    module_path = repo_root / "dim_date_seed.py"
    spec = importlib.util.spec_from_file_location("dim_date_seed", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None

    awsglue = ModuleType("awsglue")
    awsglue_context = ModuleType("awsglue.context")
    pyspark = ModuleType("pyspark")
    pyspark_context = ModuleType("pyspark.context")
    pyspark_sql = ModuleType("pyspark.sql")
    pyspark_sql.functions = ModuleType("pyspark.sql.functions")

    class DummySparkContext:
        def __init__(self):
            pass

    class DummyGlueContext:
        def __init__(self, sc):
            self.sc = sc

        @property
        def spark_session(self):
            return "spark-session"

    pyspark_context.SparkContext = DummySparkContext
    awsglue_context.GlueContext = DummyGlueContext

    sys.modules["awsglue"] = awsglue
    sys.modules["awsglue.context"] = awsglue_context
    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.context"] = pyspark_context
    sys.modules["pyspark.sql"] = pyspark_sql
    sys.modules["pyspark.sql.functions"] = pyspark_sql.functions

    spec.loader.exec_module(module)

    spark = module.create_spark_session()
    assert spark == "spark-session"
