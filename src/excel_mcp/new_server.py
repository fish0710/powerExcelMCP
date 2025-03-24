import logging
import sys
import os
from os import path
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP, Context
import pandas as pd
import matplotlib.pyplot as plt
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
    description="用于操作Excel文件的MCP服务器, 文件地址应该使用相对地址",
)


@mcp.tool()
def get_sheet_names(filepath: str) -> List[str]:
    """获取指定Excel文件中的所有工作表名称。

    Args:
        filepath: 目标Excel文件的相对或绝对路径

    Returns:
        List[str]: 包含所有工作表名称的列表

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 文件格式无效或不是Excel文件
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.get_sheet_names(filepath)
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise


@mcp.tool()
def get_columns_excel(filepath: str, sheet_name: str) -> str:
    """获取Excel文件中指定工作表的所有列名。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要获取列名的工作表名称

    Returns:
        str: 以逗号分隔的工作表列名字符串

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 指定的工作表名称不存在
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return ", ".join(excel_handler.get_columns(filepath, sheet_name))
    except Exception as e:
        logger.error(f"Error getting Excel columns: {e}")
        raise


@mcp.tool()
def get_columns_csv(filepath: str) -> str:
    """获取CSV文件中的所有列名。

    Args:
        filepath: 目标CSV文件的相对或绝对路径

    Returns:
        str: 以逗号分隔的文件列名字符串

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 文件格式无效或不是CSV文件
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return ", ".join(csv_handler.get_columns(filepath))
    except Exception as e:
        logger.error(f"Error getting CSV columns: {e}")
        raise


@mcp.tool()
def get_missing_values_info_csv(filepath: str) -> str:
    """获取CSV文件中的缺失值信息。

    Args:
        filepath: CSV文件路径

    Returns:
        str: 包含每列的缺失值数量和缺失率的详细统计信息

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 文件格式无效或不是CSV文件
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
    columns: Optional[List[str]] = None,
    max_unique: int = 10,
) -> Dict[str, Any]:
    """获取CSV文件中指定列的唯一值分布。

    Args:
        filepath: 目标CSV文件的相对或绝对路径
        columns: 要分析的列名列表，默认分析所有列
        max_unique: 每列显示的最大唯一值数量，超出此数量仅显示统计信息

    Returns:
        Dict[str, Any]: 包含每列唯一值分布的详细信息字典

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 指定的列名不存在
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
    filepath: str, method: str = "pearson", min_correlation: float = 0.5
) -> str:
    """获取CSV文件中列之间的相关性。

    Args:
        filepath: 目标CSV文件的相对或绝对路径
        method: 相关性分析方法，支持'pearson'、'spearman'、'kendall'
        min_correlation: 相关系数阈值，仅返回相关系数绝对值大于此值的结果

    Returns:
        str: 包含列间相关性分析的详细结果字符串

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 无效的相关性计算方法或数据类型不适合计算相关性
    """
    csv_handler = CSVHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = csv_handler.read_data(csv_handler.get_file_path(filepath))
        return csv_handler.get_column_correlation(df, method, min_correlation)
    except Exception as e:
        logger.error(f"Error calculating CSV correlations: {e}")
        raise


@mcp.tool()
def get_missing_values_info_sheet(filepath: str, sheet_name: str) -> str:
    """获取Excel工作表中的数据缺失情况。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要分析的工作表名称

    Returns:
        str: 包含每列的缺失值数量和缺失率的详细统计信息

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 工作表不存在或文件格式无效
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
def get_basic_data_from_sheet(filepath: str, sheet_name: str) -> dict:
    """获取Excel工作表的完整数据概览。执行全面的数据分析，包括数据类型统计、缺失值分析、
    非空值计数等关键指标的检查。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要分析的工作表名称

    Returns:
        dict: 包含工作表数据分析结果的详细信息字典

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 工作表不存在或文件格式无效
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        # 读取数据
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )

        # 获取缺失值信息
        missing_values_info = excel_handler.get_missing_values_info(df)

        # 获取更多数据信息
        data_info = {
            "columns": list(df.columns),
            "dtypes": df.dtypes.to_dict(),
            "non_null_counts": df.count().to_dict(),
            "missing_values": missing_values_info,
            # 可以根据需要添加更多数据检查项
        }

        return data_info

    except Exception as e:
        logger.error(f"Error inspecting Excel sheet data: {e}")
        raise


@mcp.tool()
def get_data_unique_values_sheet(
    filepath: str,
    sheet_name: str,
    columns: Optional[List[str]] = None,
    max_unique: int = 10,
) -> Dict[str, Any]:
    """获取Excel工作表中指定列的唯一值分布。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要分析的工作表名称
        columns: 要分析的列名列表，默认分析所有列
        max_unique: 每列显示的最大唯一值数量，超出此数量仅显示统计信息

    Returns:
        Dict[str, Any]: 包含每列唯一值分布的详细信息字典

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 工作表不存在或指定的列名不存在
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
    sheet_name: str,
    method: str = "pearson",
    min_correlation: float = 0.5,
) -> str:
    """获取Excel工作表中列之间的相关性。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要分析的工作表名称
        method: 相关性分析方法，支持'pearson'、'spearman'、'kendall'
        min_correlation: 相关系数阈值，仅返回相关系数绝对值大于此值的结果

    Returns:
        str: 包含列间相关性分析的详细结果字符串

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 工作表不存在、无效的相关性计算方法或数据类型不适合计算相关性
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


@mcp.tool()
def run_code_with_log_excel_sheet(
    filepath: str, sheet_name: str, python_code: str
) -> str:
    """使用 python 代码获取数据，执行过程中，print 会被捕获
    1. 进行去重时，一定要确定去重的列是有意义的

    参数:
        filepath: Excel文件路径
        sheet_name: 要处理的工作表名称
        python_code: 要执行的Python代码 main ，第一个参数为已经加载好的 DataFrame

    返回:
        str: 执行结果信息

    异常:
        ValueError: 当Python代码格式不正确时
        TypeError: 当返回值类型不是DataFrame时
    """
    # 初始化Excel处理器
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))

    try:
        return excel_handler.run_code_only_log(
            filepath, python_code, sheet_name=sheet_name
        )
    except Exception as e:
        logger.error(f"处理Excel文件时出错: {e}")
        raise


@mcp.tool()
def modify_data_with_excel(
    filepath: str, sheet_name: str, python_code: str, result_file_path: str
) -> str:
    """执行Python代码生成Excel文件数据。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        python_code: 要执行的Python代码，是一个返回DataFrame的main函数，纯函数，避免副作用
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
def modify_data_with_csv(filepath: str, python_code: str, result_file_path: str) -> str:
    """执行Python代码生成CSV文件数据。

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
    try:
        return csv_handler.run_code(filepath, python_code, result_file_path)
    except Exception as e:
        logger.error(f"Error executing CSV code: {e}")
        raise


@mcp.tool()
def plot_data_excel(
    filepath: str,
    sheet_name: str,
    save_path: str,
    python_code: str,
) -> str:
    """生成Excel数据的可视化图表。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        save_path: 图表保存路径
        python_code: 要执行的Python代码，定义为 def main(df, plt)，可以使用 matplotlib 进行可视化, 返回 plt 对象，不用保存

    Returns:
        str: 执行结果信息，请提供给用户结果文件相对路径, 用 show_file_to_user 工具前端展示

    Raises:
        ValueError: 当图表类型不支持或数据列不存在时
        FileNotFoundError: 当文件不存在时
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code_with_plot(
            filepath, python_code, save_path, sheet_name=sheet_name
        )

    except Exception as e:
        logger.error(f"生成图表时出错: {e}")
        raise


async def run_server():
    """启动Excel MCP服务器。"""
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
