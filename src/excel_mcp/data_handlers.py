from typing import List, Dict, Union, Optional, Callable, Any, Tuple
import numpy as np
import os
import logging
import pandas as pd
import functools
from typing import List, Dict, Any
from abc import ABC, abstractmethod

logger = logging.getLogger("excel-mcp")

cache = {}


def cache_method(func):
    """
    装饰器，为实例方法添加基于文件路径和参数的缓存
    支持基于文件最后修改时间的缓存失效
    """

    @functools.wraps(func)
    def wrapper(self, filepath, *args, **kwargs):
        # 获取文件的最后修改时间
        try:
            mod_time = os.path.getmtime(filepath)
        except OSError:
            # 如果文件不存在或无法获取修改时间，则不使用缓存
            return func(self, filepath, *args, **kwargs)

        # 创建缓存键，包含文件路径、最后修改时间和额外参数
        key = (filepath, mod_time, frozenset(kwargs.items()))

        if key not in cache:
            # 缓存未命中，执行原始方法并缓存结果
            result = func(self, filepath, *args, **kwargs)
            cache[key] = result

        return cache[key]

    return wrapper


def run_python_code(python_code, exec_locals):
    exec_globals = globals()
    return exec(python_code, exec_globals, exec_locals)


class BaseDataHandler(ABC):
    """基础数据处理类，提供通用的数据操作功能"""

    def __init__(self, files_path: str):
        self.files_path = files_path

    def get_file_path(self, filename: str) -> str:
        """获取文件的完整路径
        Args:
            filename: 文件名
        Returns:
            完整的文件路径
        """
        if os.path.isabs(filename):
            return filename
        return os.path.join(self.files_path, filename)

    @abstractmethod
    def read_data(self, filepath: str, **kwargs) -> pd.DataFrame:
        """读取数据文件"""
        pass

    @abstractmethod
    def write_data(self, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """写入数据到文件"""
        pass

    def run_code(
        self, filepath: str, python_code: str, result_file_path: str, **kwargs
    ) -> str:
        """执行Python代码处理数据
        Args:
            filepath: 输入文件路径
            python_code: 要执行的Python代码
            result_file_path: 结果文件路径
            **kwargs: 额外的参数
        Returns:
            执行结果信息
        """
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)
            # 准备执行环境

            exec_locals = {"df": df, "pd": pd}

            # 执行Python代码
            run_python_code(python_code, exec_locals)
            if "main" not in exec_locals:
                raise ValueError("代码中必须定义main函数")
            # 执行main函数并获取结果
            result_df = exec_locals["main"](df)
            if not isinstance(result_df, pd.DataFrame):
                raise TypeError("main函数必须返回DataFrame类型")
            # 保存结果
            self.write_data(result_df, self.get_file_path(result_file_path), **kwargs)
            return "执行完成 " + result_file_path
        except Exception as e:
            logger.error(f"Error running code: {e}")
            return f"Error: {str(e)}"

    def run_code_only_log(self, filepath: str, python_code: str, **kwargs) -> str:
        """执行Python代码处理数据
        Args:
            filepath: 输入文件路径
            python_code: 要执行的Python代码
            **kwargs: 额外的参数
        Returns:
            执行结果信息
        """
        import io
        import sys
        from contextlib import redirect_stdout

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # 创建字符串IO对象来捕获标准输出
            output_buffer = io.StringIO()

            exec_locals = {"df": df, "pd": pd}

            # 重定向标准输出并执行Python代码
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("代码中必须定义main函数")

                # 执行main函数并获取结果
                result_df = exec_locals["main"](df)

            # 获取捕获的输出
            captured_output = output_buffer.getvalue()
            return f"{captured_output}\n{result_df}"

        except Exception as e:
            logger.error(f"Error running code: {e}")
            return f"Error: {str(e)}"

    def run_code_with_plot(
        self, filepath: str, python_code: str, save_path: str, **kwargs
    ) -> str:
        """执行带有matplotlib绘图功能的Python代码
        Args:
            filepath: 输入文件路径
            python_code: 要执行的Python代码
            save_path: 图表保存路径，如果不提供则返回base64编码的图片
            **kwargs: 额外的参数
        Returns:
            执行结果信息和图表数据
        """
        import io
        from contextlib import redirect_stdout
        import matplotlib.pyplot as plt
        import matplotlib as mpl

        # 设置中文字体 sudo apt install fonts-wqy-zenhei
        mpl.rcParams["font.sans-serif"] = [
            "PingFang SC",
            "WenQuanYi Zen Hei",
            "Microsoft YaHei",
            "Arial Unicode MS",
        ]
        mpl.rcParams["axes.unicode_minus"] = False  # 解决负号显示问题

        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)

            # 创建字符串IO对象来捕获标准输出
            output_buffer = io.StringIO()

            exec_locals = {"df": df, "pd": pd, "plt": plt}

            # 重定向标准输出并执行Python代码
            with redirect_stdout(output_buffer):
                run_python_code(python_code, exec_locals)

                if "main" not in exec_locals:
                    raise ValueError("代码中必须定义main函数")

                # 执行main函数
                exec_locals["main"](df, plt)

            # 获取捕获的输出
            captured_output = output_buffer.getvalue()
            print(captured_output)

            # 确保目标目录存在
            save_full_path = self.get_file_path(save_path)
            os.makedirs(os.path.dirname(save_full_path), exist_ok=True)
            # 保存图表到文件
            plt.savefig(save_full_path)
            plt.close()
            return f"{captured_output}\n图表已保存到: {save_path}"

        except Exception as e:
            logger.error(f"Error running code with plot: {e}")
            return f"Error: {str(e)}"
        finally:
            plt.close("all")

    def get_column_correlation(
        self, df: pd.DataFrame, method: str = "pearson", min_correlation: float = 0.5
    ) -> str:
        """计算DataFrame中数值列之间的相关性。

        Args:
            df: 输入的DataFrame
            method: 相关性计算方法，支持'pearson'、'spearman'、'kendall'
            min_correlation: 相关系数阈值，仅返回相关系数绝对值大于此值的结果

        Returns:
            str: 包含列间相关性分析的详细结果字符串
        """
        try:
            # 获取数值类型的列
            numeric_cols = df.select_dtypes(include=["int64", "float64"]).columns
            if len(numeric_cols) < 2:
                return "没有足够的数值列来计算相关性"

            # 计算相关性矩阵
            correlation_matrix = df[numeric_cols].corr(method=method)

            # 筛选显著相关的结果
            significant_correlations = []
            for i in range(len(numeric_cols)):
                for j in range(i + 1, len(numeric_cols)):
                    corr = correlation_matrix.iloc[i, j]
                    if abs(corr) >= min_correlation:
                        significant_correlations.append(
                            f"{numeric_cols[i]} 和 {numeric_cols[j]} 的相关系数为: {corr:.4f}"
                        )

            if not significant_correlations:
                return f"没有找到相关系数绝对值大于{min_correlation}的列对"

            return "\n".join(significant_correlations)

        except Exception as e:
            logger.error(f"计算相关性时出错: {e}")
            raise

    def inspect_data(
        self, filepath: str, preview_rows: int = 5, preview_type: str = "head", **kwargs
    ) -> str:
        """查看数据的基本信息
        Args:
            filepath: 文件路径
            preview_rows: 预览行数
            preview_type: 预览类型
            **kwargs: 额外的参数
        Returns:
            数据信息的字符串描述
        """
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, **kwargs)
            result = []
            # 数据预览
            result.append("=== 数据预览 ===")
            preview = (
                df.head(preview_rows)
                if preview_type == "head"
                else df.tail(preview_rows)
            )
            result.append(str(preview))
            # 数据基本信息
            result.append("\n=== 数据基本信息 ===")
            result.append(f"行数: {df.shape[0]}")
            result.append(f"列数: {df.shape[1]}")
            result.append(f"列名: {list(df.columns)}")
            # 数据类型信息
            result.append("\n=== 数据类型信息 ===")
            result.append(str(df.dtypes))
            # 统计摘要
            result.append("\n=== 统计摘要 ===")
            result.append(str(df.describe()))
            return "\n".join(result)
        except Exception as e:
            logger.error(f"Error inspecting data: {e}")
            return f"Error: {str(e)}"

    def get_missing_values_info(self, df: pd.DataFrame) -> str:
        """获取缺失值信息

        Args:
            df: 数据框

        Returns:
            包含缺失值信息的数据框
        """
        missing_count = df.isnull().sum()
        missing_percent = (missing_count / len(df) * 100).round(4)

        missing_info = pd.DataFrame(
            {"缺失值数量": missing_count, "缺失率(%)": missing_percent}
        )

        return missing_info.sort_values("缺失值数量", ascending=False).to_string()

    def get_data_unique_values(
        self,
        df: pd.DataFrame,
        columns: Optional[List[str]] = None,
        max_unique: int = 10,
    ) -> str:
        """获取指定列的唯一值信息

        Args:
            df: 数据框
            columns: 需要查看的列，默认为所有列
            max_unique: 显示的最大唯一值数量，对于唯一值超过此数的列只显示计数

        Returns:
            包含唯一值信息的字典
        """
        result = {}
        cols_to_check = columns if columns else df.columns

        for col in cols_to_check:
            if col in df.columns:
                unique_values = df[col].dropna().unique()
                unique_count = len(unique_values)

                values_list = (
                    unique_values.tolist()
                    if hasattr(unique_values, "tolist")
                    else list(unique_values)
                )
                result[col] = {
                    "count": unique_count,
                    "values": (
                        values_list[:max_unique]
                        if unique_count > max_unique
                        else values_list
                    ),
                    "message": (
                        f"超过{max_unique}个唯一值，仅显示前{max_unique}个"
                        if unique_count > max_unique
                        else ""
                    ),
                }

        return str(result)


class ExcelHandler(BaseDataHandler):
    """Excel文件处理类"""

    @cache_method
    def read_data(
        self, filepath: str, sheet_name: str = None, **kwargs
    ) -> pd.DataFrame:
        """读取Excel文件数据"""
        return pd.read_excel(
            filepath,
            sheet_name=sheet_name,
            engine="calamine",
            **kwargs,
        )

    def write_data(self, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """写入数据到Excel文件"""
        df.to_excel(filepath, index=False, **kwargs)

    def get_sheet_names(self, filepath: str) -> List[str]:
        """获取Excel文件中的所有工作表名称"""
        try:
            full_path = self.get_file_path(filepath)
            excel_file = pd.ExcelFile(full_path)
            return excel_file.sheet_names
        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            raise

    def get_columns(self, filepath: str, sheet_name: str) -> List[str]:
        """获取指定工作表的列名"""
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path, sheet_name=sheet_name)
            return df.columns.tolist()
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            raise


class CSVHandler(BaseDataHandler):
    """CSV文件处理类"""

    @cache_method
    def read_data(self, filepath: str, **kwargs) -> pd.DataFrame:
        """读取CSV文件数据"""
        return pd.read_csv(filepath, **kwargs)

    def write_data(self, df: pd.DataFrame, filepath: str, **kwargs) -> None:
        """写入数据到CSV文件"""
        df.to_csv(filepath, index=False, **kwargs)

    def get_columns(self, filepath: str) -> List[str]:
        """获取CSV文件的列名"""
        try:
            full_path = self.get_file_path(filepath)
            df = self.read_data(full_path)
            return df.columns.tolist()
        except Exception as e:
            logger.error(f"Error getting columns: {e}")
            raise
