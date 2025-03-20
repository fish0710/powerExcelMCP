import logging
import sys
import os
from os import path
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP, Context
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
    filepath: str,
    sheet_name: str,
    python_code: str,
    result_file_path: str,
    ctx: Context,
) -> str:
    """
    执行Python代码处理Excel文件数据。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        python_code: 要执行的Python代码，必须包含返回DataFrame的main函数
        result_file_path: 结果Excel文件保存路径

    Returns:
        str: 执行结果信息

    Raises:
        ValueError: 当Python代码格式不正确时
        TypeError: 当返回值类型不是DataFrame时
    """
    # Initialize handlers

    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code(
            filepath, python_code, result_file_path, sheet_name=sheet_name
        )
    except Exception as e:
        logger.error(f"Error executing Excel code: {e}")
        raise


@mcp.tool()
def run_code_with_csv(
    filepath: str, python_code: str, result_file_path: str, ctx: Context
) -> str:
    """
    执行Python代码处理CSV文件数据。

    Args:
        filepath: CSV文件路径
        python_code: 要执行的Python代码，必须包含返回DataFrame的main函数
        result_file_path: 结果CSV文件保存路径

    Returns:
        str: 执行结果信息

    Raises:
        ValueError: 当Python代码格式不正确时
        TypeError: 当返回值类型不是DataFrame时
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    print(ctx)
    try:
        return csv_handler.run_code(filepath, python_code, result_file_path)
    except Exception as e:
        logger.error(f"Error executing CSV code: {e}")
        raise


@mcp.tool()
def get_sheet_names(filepath: str, ctx: Context) -> List[str]:
    """
    获取Excel文件中所有工作表的名称。

    Args:
        filepath: Excel文件路径

    Returns:
        List[str]: 工作表名称列表

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当文件不是有效的Excel文件时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.get_sheet_names(filepath)
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise


@mcp.tool()
def get_columns_excel(filepath: str, sheet_name: str, ctx: Context) -> str:
    """
    获取Excel文件指定工作表的所有列名。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称

    Returns:
        str: 以逗号分隔的列名字符串

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当工作表不存在时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return ", ".join(excel_handler.get_columns(filepath, sheet_name))
    except Exception as e:
        logger.error(f"Error getting Excel columns: {e}")
        raise


@mcp.tool()
def get_columns_csv(filepath: str, ctx: Context) -> str:
    """
    获取CSV文件的所有列名。

    Args:
        filepath: CSV文件路径

    Returns:
        str: 以逗号分隔的列名字符串

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当文件不是有效的CSV文件时
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return ", ".join(csv_handler.get_columns(filepath))
    except Exception as e:
        logger.error(f"Error getting CSV columns: {e}")
        raise


@mcp.tool()
def get_missing_values_info_csv(filepath: str, ctx: Context) -> str:
    """获取CSV文件中的缺失值信息。

    Args:
        filepath: CSV文件路径

    Returns:
        str: 包含每列缺失值数量和缺失率的信息

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当文件格式不正确时
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = csv_handler.read_data(csv_handler.get_file_path(filepath))
        return csv_handler.get_missing_values_info(df)
    except Exception as e:
        logger.error(f"Error getting CSV missing values info: {e}")
        raise


@mcp.tool()
def get_data_unique_values_csv(
    filepath: str,
    ctx: Context,
    columns: Optional[List[str]] = None,
    max_unique: int = 10,
) -> Dict[str, Any]:
    """获取CSV文件中指定列的唯一值信息。

    Args:
        filepath: CSV文件路径
        columns: 需要查看的列名列表，默认为所有列
        max_unique: 显示的最大唯一值数量，超过此数量的列只显示计数

    Returns:
        Dict[str, Any]: 包含每列唯一值信息的字典

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当指定的列不存在时
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = csv_handler.read_data(csv_handler.get_file_path(filepath))
        return csv_handler.get_data_unique_values(df, columns, max_unique)
    except Exception as e:
        logger.error(f"Error getting CSV unique values: {e}")
        raise


@mcp.tool()
def get_column_correlation_csv(
    filepath: str, ctx: Context, method: str = "pearson", min_correlation: float = 0.5
) -> str:
    """获取CSV文件中列之间的相关性。

    Args:
        filepath: CSV文件路径
        method: 相关性计算方法，可选值为'pearson'、'spearman'、'kendall'
        min_correlation: 最小相关系数阈值，只返回相关系数大于此值的结果
        ctx: MCP上下文对象

    Returns:
        str: 包含列之间相关性分析结果的字符串

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当相关性计算方法不正确或数据类型不适合计算相关性时
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = csv_handler.read_data(csv_handler.get_file_path(filepath))
        return csv_handler.get_column_correlation(df, method, min_correlation)
    except Exception as e:
        logger.error(f"Error calculating CSV correlations: {e}")
        raise


@mcp.tool()
def get_missing_values_info_sheet(filepath: str, sheet_name: str, ctx: Context) -> str:
    """获取Excel工作表中的缺失值信息。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        ctx: MCP上下文对象

    Returns:
        str: 包含每列缺失值数量和缺失率的信息

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当工作表不存在或文件格式不正确时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_missing_values_info(df)
    except Exception as e:
        logger.error(f"Error getting Excel sheet missing values info: {e}")
        raise


@mcp.tool()
def get_data_unique_values_sheet(
    filepath: str,
    ctx: Context,
    sheet_name: str,
    columns: Optional[List[str]] = None,
    max_unique: int = 10,
) -> Dict[str, Any]:
    """获取Excel工作表中指定列的唯一值信息。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        columns: 需要查看的列名列表，默认为所有列
        max_unique: 显示的最大唯一值数量，超过此数量的列只显示计数
        ctx: MCP上下文对象

    Returns:
        Dict[str, Any]: 包含每列唯一值信息的字典

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当工作表不存在或指定的列不存在时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_data_unique_values(df, columns, max_unique)
    except Exception as e:
        logger.error(f"Error getting Excel sheet unique values: {e}")
        raise


@mcp.tool()
def get_column_correlation_sheet(
    filepath: str,
    ctx: Context,
    sheet_name: str,
    method: str = "pearson",
    min_correlation: float = 0.5,
) -> str:
    """获取Excel工作表中列之间的相关性。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        method: 相关性计算方法，可选值为'pearson'、'spearman'、'kendall'
        min_correlation: 最小相关系数阈值，只返回相关系数大于此值的结果
        ctx: MCP上下文对象

    Returns:
        str: 包含列之间相关性分析结果的字符串

    Raises:
        FileNotFoundError: 当文件不存在时
        ValueError: 当工作表不存在、相关性计算方法不正确或数据类型不适合计算相关性时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        return excel_handler.get_column_correlation(df, method, min_correlation)
    except Exception as e:
        logger.error(f"Error calculating Excel sheet correlations: {e}")
        raise


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
