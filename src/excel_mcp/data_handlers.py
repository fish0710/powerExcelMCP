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
            self.write_data(result_df, self.get_file_path(result_file_path), **kwargs)

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
