import sys
import os
from dotenv import load_dotenv
load_dotenv()

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
 
os.environ["_JAVA_OPTIONS"] = "-Djava.security.manager=allow"
os.environ["JAVA_TOOL_OPTIONS"] = "-Djava.security.manager=allow"

if sys.platform == "win32":
    HADOOP_HOME = os.environ.get("HADOOP_HOME", "C:/hadoop")
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] = os.environ.get("PATH", "") + f";{HADOOP_HOME}/bin"

    _winutils = os.path.join(HADOOP_HOME, "bin", "winutils.exe")
    if not os.path.isfile(_winutils):
        raise RuntimeError(
            f"winutils.exe not found at {_winutils}. "
            "Download it from https://github.com/cdarlint/winutils "
            "and place it in C:/hadoop/bin/"
        )
        
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_PATH = "data/raw/*.parquet"
WAREHOUSE_PATH = "data/warehouse/"
REJECTED_PATH = "data/rejected/"
DDL_OUTPUT_PATH = "sql/schema_report.sql"
SPARK_JVM_OPTIONS = "-Djava.security.manager=allow"
