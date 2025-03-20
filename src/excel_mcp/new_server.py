import logging
import sys
import os
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP
import pandas as pd
from .data_handlers import ExcelHandler, CSVHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("excel-mcp.log")],
    force=True,
)

logger = logging.getLogger("excel-mcp")

# Get Excel files path from environment or use default
EXCEL_FILES_PATH = os.environ.get("EXCEL_FILES_PATH", "./excel_files")

# Create the directory if it doesn't exist
os.makedirs(EXCEL_FILES_PATH, exist_ok=True)

# Initialize handlers
excel_handler = ExcelHandler(EXCEL_FILES_PATH)
csv_handler = CSVHandler(EXCEL_FILES_PATH)

# Initialize FastMCP server
mcp = FastMCP(
    "excel-mcp",
    version="0.1.0",
    description="用于操作Excel文件的MCP服务器",
    dependencies=["openpyxl>=3.1.2"],
    env_vars={
        "EXCEL_FILES_PATH": {
            "description": "Path to Excel files directory",
            "required": False,
            "default": EXCEL_FILES_PATH,
        }
    },
)


@mcp.tool()
def run_code_with_excel(
    filepath: str, sheet_name: str, python_code: str, result_file_path: str
):
    """
    写一个 python 代码，入参为 filepath 获取到的 DataFrame 对象，返回一个 DataFrame, 我会帮你写入文件。
    Args:
        python_code: 要执行的Python代码, 格式为 def main(origin_df): 返回 pd.DataFrame
        result_file_path: 结果 Excel文件路径

    Returns:
        执行结果

    """
    return excel_handler.run_code(
        filepath, python_code, result_file_path, sheet_name=sheet_name
    )


@mcp.tool()
def run_code_with_csv(filepath: str, python_code: str, result_file_path: str):
    """
    写一个数据处理 python 代码，入参为 filepath 获取到的 DataFrame 对象，返回一个 DataFrame, 我会帮你写入文件。
    注意 print 不会被记录，不能使用 print 打印数据。注意换行。
    Args:
        filepath: csv 文件路径
        python_code: 要执行的Python代码, 格式为 def main(origin_df): 返回 pd.DataFrame
        result_file_path: 结果 csv 文件路径

    Returns:
        执行结果

    """
    return csv_handler.run_code(filepath, python_code, result_file_path)


@mcp.tool()
def get_sheet_names(filepath: str) -> List[str]:
    """Get names of all sheets in Excel file."""
    return excel_handler.get_sheet_names(filepath)


@mcp.tool()
def get_columns_excel(filepath: str, sheet_name: str) -> str:
    """Get names of all sheets in Excel file."""
    return ", ".join(excel_handler.get_columns(filepath, sheet_name))


@mcp.tool()
def get_columns_csv(filepath: str) -> str:
    """Get names of all columns in CSV file."""
    return ", ".join(csv_handler.get_columns(filepath))


@mcp.tool()
def get_missing_values_info_csv(filepath: str) -> str:
    """获取缺失值信息

    Args:

    Returns:
        包含缺失值信息的数据框
    """
    return csv_handler.get_missing_values_info(
        csv_handler.read_data(csv_handler.get_file_path(filepath))
    )


@mcp.tool()
def get_data_unique_values_csv(
    filepath: str, columns: Optional[List[str]] = None, max_unique: int = 10
) -> Dict[str, Any]:
    """获取指定列的唯一值信息

    Args:
        columns: 需要查看的列，默认为所有列
        max_unique: 显示的最大唯一值数量，对于唯一值超过此数的列只显示计数

    Returns:
        包含唯一值信息的字典
    """
    return csv_handler.get_data_unique_values(
        csv_handler.read_data(csv_handler.get_file_path(filepath))
    )


@mcp.tool()
def get_column_correlation_csv(
    filepath: str, method: str = "pearson", min_correlation: float = 0.5
) -> str:
    """获取列之间的相关性

    Args:
        df: 数据框
        method: 相关系数计算方法，'pearson', 'kendall', 或 'spearman'
        min_correlation: 最小相关系数阈值，只返回绝对值大于此值的相关性

    Returns:
        相关系数矩阵
    """
    return csv_handler.get_data_unique_values(
        csv_handler.read_data(csv_handler.get_file_path(filepath))
    )


@mcp.tool()
def get_missing_values_info_sheet(filepath: str) -> str:
    """获取缺失值信息

    Args:

    Returns:
        包含缺失值信息的数据框
    """
    return excel_handler.get_missing_values_info(
        excel_handler.read_data(excel_handler.get_file_path(filepath))
    )


@mcp.tool()
def get_data_unique_values_sheet(
    filepath: str, columns: Optional[List[str]] = None, max_unique: int = 10
) -> Dict[str, Any]:
    """获取指定列的唯一值信息

    Args:
        columns: 需要查看的列，默认为所有列
        max_unique: 显示的最大唯一值数量，对于唯一值超过此数的列只显示计数

    Returns:
        包含唯一值信息的字典
    """
    return excel_handler.get_data_unique_values(
        excel_handler.read_data(
            excel_handler.get_file_path(filepath), columns, max_unique
        )
    )


@mcp.tool()
def get_column_correlation_sheet(
    filepath: str, method: str = "pearson", min_correlation: float = 0.5
) -> str:
    """获取列之间的相关性

    Args:
        method: 相关系数计算方法，'pearson', 'kendall', 或 'spearman'
        min_correlation: 最小相关系数阈值，只返回绝对值大于此值的相关性

    Returns:
        相关系数矩阵
    """
    return excel_handler.get_data_unique_values(
        excel_handler.read_data(excel_handler.get_file_path(filepath)),
        method,
        min_correlation,
    )


async def run_server():
    """Run the Excel MCP server."""
    try:
        logger.info(f"Starting Excel MCP server (files directory: {EXCEL_FILES_PATH})")
        await mcp.run_sse_async()
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
        await mcp.shutdown()
    except Exception as e:
        logger.error(f"Server failed: {e}")
        raise
    finally:
        logger.info("Server shutdown complete")
