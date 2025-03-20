from typing import List, Dict, Union, Optional, Callable, Any, Tuple
import numpy as np
import os
import logging
import pandas as pd
from typing import List, Dict, Any
from abc import ABC, abstractmethod

logger = logging.getLogger("excel-mcp")


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
            exec_globals = {"df": df, "pd": pd}
            exec_locals = {}

            # 执行Python代码
            exec(python_code, exec_globals, exec_locals)
            if "main" not in exec_locals:
                raise ValueError("代码中必须定义main函数")
            # 执行main函数并获取结果
            result_df = exec_locals["main"](df)
            if not isinstance(result_df, pd.DataFrame):
                raise TypeError("main函数必须返回DataFrame类型")
            # 保存结果
            self.write_data(result_df, self.get_file_path(
                result_file_path), **kwargs)
            return "执行完成 " + result_file_path
        except Exception as e:
            logger.error(f"Error running code: {e}")
            return f"Error: {str(e)}"

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

        missing_info = pd.DataFrame({
            '缺失值数量': missing_count,
            '缺失率(%)': missing_percent
        })

        return missing_info.sort_values('缺失值数量', ascending=False).to_string()

    def get_data_unique_values(
        self, df: pd.DataFrame, columns: Optional[List[str]] = None, max_unique: int = 10
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

                result[col] = {
                    'count': unique_count,
                    'values': unique_values.tolist() if hasattr(unique_values, 'tolist') else list(unique_values),
                    'message':  f'超过{max_unique}个唯一值，不全部显示' if unique_count <= max_unique else ""
                }

        return str(result)

    # def get_column_correlation(
    #     self, df: pd.DataFrame, method: str = 'pearson', min_correlation: float = 0.5
    # ) -> pd.DataFrame:
    #     """获取列之间的相关性

    #     Args:
    #         df: 数据框
    #         method: 相关系数计算方法，'pearson', 'kendall', 或 'spearman'
    #         min_correlation: 最小相关系数阈值，只返回绝对值大于此值的相关性

    #     Returns:
    #         相关系数矩阵
    #     """
    #     # 只选择数值型列
    #     numeric_df = df.select_dtypes(include=['number'])

    #     if numeric_df.empty:
    #         return pd.DataFrame()

    #     corr_matrix = numeric_df.corr(method=method)

    #     # 筛选出相关性强的列对
    #     mask = np.abs(corr_matrix) > min_correlation
    #     # 移除自相关(对角线)
    #     np.fill_diagonal(mask.values, False)

    #     # 如果没有强相关的列对，返回空DataFrame
    #     if not mask.any().any():
    #         return pd.DataFrame()

    #     # 只保留相关性强的值
    #     filtered_corr = corr_matrix.where(mask)

    #     return filtered_corr.to_string()

    # def analyze_text_column(
    #     self, df: pd.DataFrame, column: str, max_length: Optional[int] = None
    # ) -> Dict[str, Any]:
    #     """分析文本列

    #     Args:
    #         df: 数据框
    #         column: 文本列名
    #         max_length: 处理的最大长度，如果为None则处理所有行

    #     Returns:
    #         包含文本分析信息的字典
    #     """
    #     if column not in df.columns:
    #         return {'error': f'列 {column} 不存在'}

    #     # 获取列数据
    #     series = df[column].astype(str)
    #     if max_length:
    #         series = series.head(max_length)

    #     # 文本长度统计
    #     length_stats = series.str.len().describe().to_dict()

    #     # 空白文本统计
    #     empty_count = (series == '').sum() + (series.isna()).sum()

    #     # 常见值及出现频率
    #     value_counts = series.value_counts().head(10).to_dict()

    #     # 包含特殊字符的行数
    #     special_chars_pattern = r'[!@#$%^&*(),.?":{}|<>]'
    #     has_special_chars = series.str.contains(
    #         special_chars_pattern, regex=True).sum()

    #     # 全数字的行数
    #     is_numeric = series.str.match(r'^\d+$').sum()

    #     return {
    #         'length_stats': length_stats,
    #         'empty_count': empty_count,
    #         'empty_percent': (empty_count / len(series) * 100).round(2),
    #         'top_values': value_counts,
    #         'has_special_chars': has_special_chars,
    #         'special_chars_percent': (has_special_chars / len(series) * 100).round(2),
    #         'numeric_only_count': is_numeric,
    #         'numeric_only_percent': (is_numeric / len(series) * 100).round(2)
    #     }

    # def detect_outliers(
    #     self, df: pd.DataFrame, columns: Optional[List[str]] = None, method: str = 'iqr', threshold: float = 1.5
    # ) -> Dict[str, pd.DataFrame]:
    #     """检测异常值

    #     Args:
    #         df: 数据框
    #         columns: 需要检测的列，默认为所有数值列
    #         method: 检测方法，'iqr'使用四分位距法，'zscore'使用Z分数法
    #         threshold: 阈值，IQR法的倍数或Z分数法的标准差倍数

    #     Returns:
    #         包含异常值信息的字典，每个列作为一个键，值为异常值数据框
    #     """
    #     # 只选择数值型列
    #     numeric_df = df.select_dtypes(include=['number'])

    #     if numeric_df.empty:
    #         return {}

    #     cols_to_check = columns if columns else numeric_df.columns
    #     cols_to_check = [
    #         col for col in cols_to_check if col in numeric_df.columns]

    #     result = {}

    #     for col in cols_to_check:
    #         series = df[col].dropna()

    #         if method == 'iqr':
    #             # IQR法
    #             q1 = series.quantile(0.25)
    #             q3 = series.quantile(0.75)
    #             iqr = q3 - q1

    #             lower_bound = q1 - threshold * iqr
    #             upper_bound = q3 + threshold * iqr

    #             outliers = df[(df[col] < lower_bound) |
    #                           (df[col] > upper_bound)]

    #             if not outliers.empty:
    #                 result[col] = {
    #                     'outliers': outliers,
    #                     'lower_bound': lower_bound,
    #                     'upper_bound': upper_bound,
    #                     'count': len(outliers)
    #                 }

    #         elif method == 'zscore':
    #             # Z分数法
    #             mean = series.mean()
    #             std = series.std()

    #             if std == 0:  # 避免除以零
    #                 continue

    #             z_scores = np.abs((series - mean) / std)
    #             outliers = df[z_scores > threshold]

    #             if not outliers.empty:
    #                 result[col] = {
    #                     'outliers': outliers,
    #                     'mean': mean,
    #                     'std': std,
    #                     'threshold': threshold,
    #                     'count': len(outliers)
    #                 }

    #     return result


class ExcelHandler(BaseDataHandler):
    """Excel文件处理类"""

    def read_data(
        self, filepath: str, sheet_name: str = None, **kwargs
    ) -> pd.DataFrame:
        """读取Excel文件数据"""
        return pd.read_excel(filepath, sheet_name=sheet_name, **kwargs)

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
