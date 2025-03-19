import logging
import sys
import os
from typing import Any, List, Dict

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
    写一个 python 代码，入参为 filepath 获取到的 DataFrame 对象，返回一个 DataFrame, 我会帮你写入文件。
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
def get_columns_excel(filepath: str, sheet_name: str) -> List[str]:
    """Get names of all sheets in Excel file."""
    return excel_handler.get_columns(filepath, sheet_name)


@mcp.tool()
def get_columns_csv(filepath: str) -> List[str]:
    """Get names of all columns in CSV file."""
    return csv_handler.get_columns(filepath)


@mcp.tool()
def inspect_data_in_sheet(
    filepath: str, sheet_name: str, preview_rows: int = 5, preview_type: str = "head"
) -> str:
    """查看Excel数据的基本信息，包括数据预览、统计摘要和数据结构。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        preview_rows: 预览行数，默认5行
        preview_type: 预览类型，'head'查看前几行，'tail'查看后几行

    Returns:
        包含数据信息的格式化字符串
    """
    return excel_handler.inspect_data(
        filepath, preview_rows, preview_type, sheet_name=sheet_name
    )


@mcp.tool()
def inspect_data_in_csv(
    filepath: str, preview_rows: int = 5, preview_type: str = "head"
) -> str:
    """查看CSV数据的基本信息，包括数据预览、统计摘要和数据结构。

    Args:
        filepath: CSV文件路径
        preview_rows: 预览行数，默认5行
        preview_type: 预览类型，'head'查看前几行，'tail'查看后几行

    Returns:
        包含数据信息的格式化字符串
    """
    return csv_handler.inspect_data(filepath, preview_rows, preview_type)


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
