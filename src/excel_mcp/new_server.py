import logging
import sys
import os
from os import path
from typing import Any, List, Dict, Optional
from mcp.server.fastmcp import FastMCP, Context
import pandas as pd
import matplotlib.pyplot as plt
from .data_handlers import ExcelHandler

os.environ["MODIN_ENGINE"] = "dask"
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
def list_worksheets(filepath: str) -> List[str]:
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
        sheet_names = excel_handler.get_sheet_names(filepath)
        return f"共 {len(sheet_names)} 个\n" + "\n".join(sheet_names)
    except Exception as e:
        logger.error(f"Error getting sheet names: {e}")
        raise


def analyze_data_overview(filepath: str, sheet_name: str) -> str:
    """数据分析首选：获取Excel工作表的完整数据概览。执行全面的数据分析，包括数据类型统计、缺失值分析、
    非空值计数等关键指标的检查。
    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要分析的工作表名称
    Returns:
        str: 包含工作表数据分析结果的详细信息字符串
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
        # 获取行列数据
        num_rows, num_cols = df.shape
        # 获取缺失值信息
        missing_values_info = excel_handler.get_missing_values_info(df)

        # 将数据类型信息转换为字符串格式
        dtypes_str = "\n".join(
            [f"    {col}: {dtype}" for col, dtype in df.dtypes.items()]
        )

        # 将非空值计数转换为字符串格式
        non_null_str = "\n".join(
            [f"    {col}: {count}" for col, count in df.count().items()]
        )

        # 将缺失值信息转换为字符串
        if isinstance(missing_values_info, dict):
            missing_values_str = "\n".join(
                [f"    {col}: {info}" for col, info in missing_values_info.items()]
            )
        else:
            missing_values_str = str(missing_values_info)

        # 构建最终的结果字符串
        result = f"""
数据分析结果：
1. 数据规模:
    总行数: {num_rows}
    总列数: {num_cols}

2. 数据类型:
{dtypes_str}

3. 非空值计数:
{non_null_str}

4. 缺失值分析:
{missing_values_str}
"""
        return result
    except Exception as e:
        error_msg = f"Error inspecting Excel sheet data: {e}"
        logger.error(error_msg)
        return error_msg


@mcp.tool()
def list_columns(filepath: str, sheet_name: str) -> str:
    """获取Excel文件中指定工作表的所有列名及其数据类型。

    Args:
        filepath: 目标Excel文件的相对或绝对路径
        sheet_name: 要获取列名的工作表名称

    Returns:
        str: 包含列名和数据类型的格式化字符串

    Raises:
        FileNotFoundError: 指定的文件路径不存在
        ValueError: 指定的工作表名称不存在
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        columns = df.columns.tolist()
        dtypes = df.dtypes
        result = [f"{col}\t{dtypes[col]}" for col in columns]
        return "共" + str(len(columns)) + "列\n" + ", ".join(result)
    except Exception as e:
        logger.error(f"Error getting Excel columns: {e}")
        raise


@mcp.tool()
def analyze_missing_values(filepath: str, sheet_name: str) -> str:
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
def analyze_unique_values(
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
def analyze_correlations(
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
def print_data_log(filepath: str, sheet_name: str, python_code: str) -> str:
    """用于执行Python代码并捕获输出的数据观察工具。

    此工具专门用于数据探索和调试，通过执行自定义Python代码来观察和分析DataFrame数据。
    所有print语句的输出都会被捕获并返回，便于查看中间计算结果。

    使用限制：
    1. 仅用于数据观察，不能修改源文件或生成新文件
    2. 不支持图表绘制功能（请使用visualize_data）
    3. 代码执行结果仅用于显示，不可被其他函数调用
    4. 建议优化print输出，避免冗长的数据打印

    Args:
        filepath: Excel文件的相对或绝对路径
        sheet_name: 要处理的工作表名称
        python_code: 包含main函数的Python代码，main函数接收一个DataFrame参数

    Returns:
        str: 包含所有print输出和main函数返回值的字符串

    Raises:
        ValueError: Python代码格式错误或缺少main函数
        TypeError: main函数返回值类型错误

    Example:
        代码示例：
        ```python
        def main(df):
            # 显示前5行数据
            print(df.head())
        ```
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
def save_transformed_data(
    filepath: str,
    sheet_name: str,
    python_code: str,
    result_file_path: str,
    default_sheet_name: str = "Sheet1",
) -> str:
    """执行Python代码生成Excel文件数据，支持单表或多表处理。

    Args:
        filepath: Excel文件路径
        sheet_name: 源工作表名称
        python_code: 要执行的Python代码，是一个返回DataFrame或Dict[str, DataFrame]的main函数。
                   当返回DataFrame时，数据将保存到default_sheet_name指定的工作表中；
                   当返回Dict[str, DataFrame]时，字典的键为工作表名称，值为对应的DataFrame。
                   函数应为纯函数，避免副作用。
        result_file_path: 结果Excel文件保存路径
        default_sheet_name: 默认工作表名称，当python_code返回单个DataFrame时使用
    Returns:
        str: 执行结果信息，包含生成的工作表信息

    Example:
        # 返回单个DataFrame的示例
        def main(df):
            return df.groupby('category').sum()

        # 返回多个工作表的示例
        def main(df):
            monthly = df.groupby('month').sum()
            yearly = df.groupby('year').sum()
            return {
                'monthly_summary': monthly,
                'yearly_summary': yearly
            }
    """
    # Initialize handlers

    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        return excel_handler.run_code(
            filepath,
            python_code,
            sheet_name=sheet_name,
            result_file_path=result_file_path,
            result_sheet_name=default_sheet_name,
        )
    except Exception as e:
        logger.error(f"Error executing Excel code: {e}")
        raise


@mcp.tool()
def visualize_data(
    filepath: str,
    sheet_name: str,
    save_path: str,
    python_code: str,
) -> str:
    """绘制Excel数据的可视化图表专用函数。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        save_path: 图表保存路径
        python_code: 要执行的Python代码，定义为 def main(df, plt)，可以使用 matplotlib 进行可视化, 返回 plt 对象，不用保存

    Returns:
        str: 执行结果信息，请提供给用户结果文件相对路径

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


@mcp.tool()
def analyze_numeric_stats(
    filepath: str, sheet_name: str, columns: List[str]
) -> Dict[str, Any]:
    """获取数值列的统计信息，包括均值、中位数、标准差、分位数等。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        columns: 要分析的列名列表，默认分析所有数值列，至少一列。所有列都必须是数值类型且中文名称。最多 10 列。

    Returns:
        Dict[str, Any]: 包含每个数值列统计信息的字典
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        numerical_cols = (
            df.select_dtypes(include=["int64", "float64"]).columns
            if columns is None
            else columns
        )

        # 获取基本统计信息
        stats = df[numerical_cols].describe()

        # 添加求和信息
        sums = df[numerical_cols].sum(skipna=True)[0]
        stats.loc["sum"] = sums

        return stats.to_dict()
    except Exception as e:
        logger.error(f"计算统计信息时出错: {e}")
        raise


@mcp.tool()
def analyze_group_stats(
    filepath: str,
    sheet_name: str,
    group_by: str,
    agg_columns: List[str],
    agg_functions: List[str] = ["mean", "count"],
) -> str:
    """按指定列分组并计算统计信息。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        group_by: 用于分组的列名
        agg_columns: 需要统计的列名列表
        agg_functions: 统计函数列表，支持 'mean', 'sum', 'count', 'min', 'max' 等

    Returns:
        str: 分组统计结果的字符串表示
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        grouped = df.groupby(group_by)[agg_columns].agg(agg_functions)
        # 按第一个统计列的第一个聚合函数结果降序排序
        first_col = agg_columns[0]
        first_func = agg_functions[0]
        sort_col = (
            (first_col, first_func)
            if isinstance(grouped.columns, pd.MultiIndex)
            else first_col
        )
        sorted_grouped = grouped.sort_values(by=sort_col, ascending=False)
        return sorted_grouped.to_string()
    except Exception as e:
        logger.error(f"分组统计时出错: {e}")
        raise


@mcp.tool()
def analyze_time_series(
    filepath: str, sheet_name: str, date_column: str, value_column: str, freq: str = "M"
) -> str:
    """对时间序列数据进行分析，包括趋势、季节性等。

    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        date_column: 日期列名
        value_column: 值列名
        freq: 重采样频率，如'D'(天),'M'(月),'Y'(年)

    Returns:
        str: 时间序列分析结果
    """
    excel_handler = ExcelHandler(path.join(EXCEL_FILES_PATH, ""))
    try:
        df = excel_handler.read_data(
            excel_handler.get_file_path(filepath), sheet_name=sheet_name
        )
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.set_index(date_column)

        # 重采样并计算统计值
        resampled = df[value_column].resample(freq).agg(["mean", "min", "max", "count"])

        return f"时间序列分析结果（频率：{freq}）：\n{resampled.to_string()}"
    except Exception as e:
        logger.error(f"时间序列分析时出错: {e}")
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
