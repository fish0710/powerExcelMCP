import logging
import sys
import os
from typing import Any, List, Dict

from mcp.server.fastmcp import FastMCP

# Import exceptions
from excel_mcp.exceptions import (
    ValidationError,
    WorkbookError,
    SheetError,
    DataError,
    FormattingError,
    CalculationError,
    PivotError,
    ChartError
)

# Import from excel_mcp package with consistent _impl suffixes
from excel_mcp.validation import (
    validate_formula_in_cell_operation as validate_formula_impl,
    validate_range_in_sheet_operation as validate_range_impl
)
from excel_mcp.chart import create_chart_in_sheet as create_chart_impl
from excel_mcp.workbook import get_workbook_info
from excel_mcp.data import write_data
from excel_mcp.pivot import create_pivot_table as create_pivot_table_impl
from excel_mcp.sheet import (
    copy_sheet,
    delete_sheet,
    rename_sheet,
    merge_range,
    unmerge_range,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("excel-mcp.log")
    ],
    force=True
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
    description="Excel MCP 服务器，用于处理Excel文件",
    dependencies=["openpyxl>=3.1.2"],
    env_vars={
        "EXCEL_FILES_PATH": {
            "description": "Excel文件存储路径",
            "required": False,
            "default": EXCEL_FILES_PATH
        }
    }
)

def get_excel_path(filename: str) -> str:
    """获取Excel文件的完整路径。
    
    Args:
        filename: Excel文件名
        
    Returns:
        Excel文件的完整路径
    """
    # If filename is already an absolute path, return it
    if os.path.isabs(filename):
        return filename
        
    # Use the configured Excel files path
    return os.path.join(EXCEL_FILES_PATH, filename)

@mcp.tool()
def apply_formula(
    filepath: str,
    sheet_name: str,
    cell: str,
    formula: str,
) -> str:
    """将Excel公式应用到单元格。"""
    try:
        full_path = get_excel_path(filepath)
        # First validate the formula
        validation = validate_formula_impl(full_path, sheet_name, cell, formula)
        if isinstance(validation, dict) and "error" in validation:
            return f"错误: {validation['error']}"
            
        # If valid, apply the formula
        from excel_mcp.calculations import apply_formula as apply_formula_impl
        result = apply_formula_impl(full_path, sheet_name, cell, formula)
        return result["message"]
    except (ValidationError, CalculationError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"应用公式时出错: {e}")
        raise

@mcp.tool()
def validate_formula_syntax(
    filepath: str,
    sheet_name: str,
    cell: str,
    formula: str,
) -> str:
    """验证Excel公式语法，不实际应用它。"""
    try:
        full_path = get_excel_path(filepath)
        result = validate_formula_impl(full_path, sheet_name, cell, formula)
        return result["message"]
    except (ValidationError, CalculationError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"验证公式时出错: {e}")
        raise

@mcp.tool()
def format_range(
    filepath: str,
    sheet_name: str,
    start_cell: str,
    end_cell: str = None,
    bold: bool = False,
    italic: bool = False,
    underline: bool = False,
    font_size: int = None,
    font_color: str = None,
    bg_color: str = None,
    border_style: str = None,
    border_color: str = None,
    number_format: str = None,
    alignment: str = None,
    wrap_text: bool = False,
    merge_cells: bool = False,
    protection: Dict[str, Any] = None,
    conditional_format: Dict[str, Any] = None
) -> str:
    """对单元格区域应用格式化。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.formatting import format_range as format_range_func
        
        result = format_range_func(
            filepath=full_path,
            sheet_name=sheet_name,
            start_cell=start_cell,
            end_cell=end_cell,
            bold=bold,
            italic=italic,
            underline=underline,
            font_size=font_size,
            font_color=font_color,
            bg_color=bg_color,
            border_style=border_style,
            border_color=border_color,
            number_format=number_format,
            alignment=alignment,
            wrap_text=wrap_text,
            merge_cells=merge_cells,
            protection=protection,
            conditional_format=conditional_format
        )
        return "区域格式化成功"
    except (ValidationError, FormattingError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"格式化区域时出错: {e}")
        raise

@mcp.tool()
def read_data_from_excel(
    filepath: str,
    sheet_name: str,
    start_cell: str = "A1",
    end_cell: str = None,
    preview_only: bool = False
) -> str:
    """从Excel工作表读取数据。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.data import read_excel_range
        result = read_excel_range(full_path, sheet_name, start_cell, end_cell, preview_only)
        if not result:
            return "在指定范围内未找到数据"
        # Convert the list of dicts to a formatted string
        data_str = "\n".join([str(row) for row in result])
        return data_str
    except Exception as e:
        logger.error(f"读取数据时出错: {e}")
        raise

@mcp.tool()
def write_data_to_excel(
    filepath: str,
    sheet_name: str,
    data: List[Dict],
    start_cell: str = "A1",
    write_headers: bool = True,
) -> str:
    """向Excel工作表写入数据。"""
    try:
        full_path = get_excel_path(filepath)
        result = write_data(full_path, sheet_name, data, start_cell, write_headers)
        return result["message"]
    except (ValidationError, DataError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"写入数据时出错: {e}")
        raise

@mcp.tool()
def create_workbook(filepath: str, upload: bool = True) -> str:
    """创建新的Excel工作簿，并可选择上传到文件服务器。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.workbook import create_workbook as create_workbook_impl
        result = create_workbook_impl(full_path, upload=upload)
        
        if upload and "file_url" in result:
            return f"已在 {full_path} 创建工作簿并上传到 {result['file_url']}"
        else:
            return f"已在 {full_path} 创建工作簿"
    except WorkbookError as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"创建工作簿时出错: {e}")
        raise

@mcp.tool()
def create_worksheet(filepath: str, sheet_name: str) -> str:
    """在工作簿中创建新的工作表。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.workbook import create_sheet as create_worksheet_impl
        result = create_worksheet_impl(full_path, sheet_name)
        return result["message"]
    except (ValidationError, WorkbookError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"创建工作表时出错: {e}")
        raise

@mcp.tool()
def create_chart(
    filepath: str,
    sheet_name: str,
    data_range: str,
    chart_type: str,
    target_cell: str,
    title: str = "",
    x_axis: str = "",
    y_axis: str = ""
) -> str:
    """在工作表中创建图表。"""
    try:
        full_path = get_excel_path(filepath)
        result = create_chart_impl(
            filepath=full_path,
            sheet_name=sheet_name,
            data_range=data_range,
            chart_type=chart_type,
            target_cell=target_cell,
            title=title,
            x_axis=x_axis,
            y_axis=y_axis
        )
        return result["message"]
    except (ValidationError, ChartError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"创建图表时出错: {e}")
        raise

@mcp.tool()
def create_pivot_table(
    filepath: str,
    sheet_name: str,
    data_range: str,
    rows: List[str],
    values: List[str],
    columns: List[str] = None,
    agg_func: str = "mean"
) -> str:
    """在工作表中创建数据透视表。"""
    try:
        full_path = get_excel_path(filepath)
        result = create_pivot_table_impl(
            filepath=full_path,
            sheet_name=sheet_name,
            data_range=data_range,
            rows=rows,
            values=values,
            columns=columns or [],
            agg_func=agg_func
        )
        return result["message"]
    except (ValidationError, PivotError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"创建数据透视表时出错: {e}")
        raise

@mcp.tool()
def copy_worksheet(
    filepath: str,
    source_sheet: str,
    target_sheet: str
) -> str:
    """在工作簿内复制工作表。"""
    try:
        full_path = get_excel_path(filepath)
        result = copy_sheet(full_path, source_sheet, target_sheet)
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"复制工作表时出错: {e}")
        raise

@mcp.tool()
def delete_worksheet(
    filepath: str,
    sheet_name: str
) -> str:
    """从工作簿中删除工作表。"""
    try:
        full_path = get_excel_path(filepath)
        result = delete_sheet(full_path, sheet_name)
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"删除工作表时出错: {e}")
        raise

@mcp.tool()
def rename_worksheet(
    filepath: str,
    old_name: str,
    new_name: str
) -> str:
    """重命名工作簿中的工作表。"""
    try:
        full_path = get_excel_path(filepath)
        result = rename_sheet(full_path, old_name, new_name)
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"重命名工作表时出错: {e}")
        raise

@mcp.tool()
def get_workbook_metadata(
    filepath: str,
    include_ranges: bool = False
) -> str:
    """获取工作簿的元数据，包括工作表、区域等信息。"""
    try:
        full_path = get_excel_path(filepath)
        result = get_workbook_info(full_path, include_ranges=include_ranges)
        return str(result)
    except WorkbookError as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"获取工作簿元数据时出错: {e}")
        raise

@mcp.tool()
def merge_cells(filepath: str, sheet_name: str, start_cell: str, end_cell: str) -> str:
    """合并单元格区域。"""
    try:
        full_path = get_excel_path(filepath)
        result = merge_range(full_path, sheet_name, start_cell, end_cell)
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"合并单元格时出错: {e}")
        raise

@mcp.tool()
def unmerge_cells(filepath: str, sheet_name: str, start_cell: str, end_cell: str) -> str:
    """取消合并单元格区域。"""
    try:
        full_path = get_excel_path(filepath)
        result = unmerge_range(full_path, sheet_name, start_cell, end_cell)
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"取消合并单元格时出错: {e}")
        raise

@mcp.tool()
def copy_range(
    filepath: str,
    sheet_name: str,
    source_start: str,
    source_end: str,
    target_start: str,
    target_sheet: str = None
) -> str:
    """复制单元格区域到另一个位置。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.sheet import copy_range_operation
        result = copy_range_operation(
            full_path,
            sheet_name,
            source_start,
            source_end,
            target_start,
            target_sheet
        )
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"复制区域时出错: {e}")
        raise

@mcp.tool()
def delete_range(
    filepath: str,
    sheet_name: str,
    start_cell: str,
    end_cell: str,
    shift_direction: str = "up"
) -> str:
    """删除单元格区域并移动剩余单元格。"""
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.sheet import delete_range_operation
        result = delete_range_operation(
            full_path,
            sheet_name,
            start_cell,
            end_cell,
            shift_direction
        )
        return result["message"]
    except (ValidationError, SheetError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"删除区域时出错: {e}")
        raise

@mcp.tool()
def validate_excel_range(
    filepath: str,
    sheet_name: str,
    start_cell: str,
    end_cell: str = None
) -> str:
    """验证Excel区域是否存在且格式正确。"""
    try:
        full_path = get_excel_path(filepath)
        range_str = start_cell if not end_cell else f"{start_cell}:{end_cell}"
        result = validate_range_impl(full_path, sheet_name, range_str)
        return result["message"]
    except ValidationError as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"验证区域时出错: {e}")
        raise

@mcp.tool()
def batch_process_excel_data(
    filepath: str,
    sheet_name: str,
    operations: List[Dict],
    save_intervals: int = 100
) -> str:
    """批量处理Excel数据，提高大数据集的性能。
    
    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        operations: 操作列表，每个操作是一个字典，包含 'type' 和其他相关参数
                  支持的操作类型: 'filter', 'transform', 'aggregate', 'sort'
        save_intervals: 每处理多少操作保存一次，提高大数据处理的可靠性
    """
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.data import batch_process_data
        result = batch_process_data(
            full_path,
            sheet_name,
            operations,
            save_intervals
        )
        return f"批量处理完成。原始行数: {result['original_row_count']}, 最终行数: {result['final_row_count']}"
    except (ValidationError, DataError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"批量处理数据时出错: {e}")
        raise

@mcp.tool()
def convert_excel_format(
    input_filepath: str,
    output_filepath: str,
    output_format: str,
    sheet_name: str = None,
    encoding: str = 'utf-8'
) -> str:
    """将Excel文件转换为其他格式，如CSV、JSON、HTML等。
    
    Args:
        input_filepath: 输入Excel文件路径
        output_filepath: 输出文件路径
        output_format: 输出格式 ('csv', 'json', 'html', 'parquet', 'feather')
        sheet_name: 要转换的工作表名称，默认为None（转换所有工作表）
        encoding: 输出文件编码
    """
    try:
        full_input_path = get_excel_path(input_filepath)
        # 对于输出路径，使用绝对路径检查
        if not os.path.isabs(output_filepath):
            full_output_path = os.path.join(EXCEL_FILES_PATH, output_filepath)
        else:
            full_output_path = output_filepath
            
        from excel_mcp.data import convert_format
        result = convert_format(
            full_input_path,
            full_output_path,
            output_format,
            sheet_name,
            encoding
        )
        
        if "output_files" in result:
            return f"已转换为 {output_format.upper()} 格式: {', '.join(result['output_files'])}"
        else:
            return f"已转换为 {output_format.upper()} 格式: {result['output_file']}"
    except (ValidationError, DataError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"转换格式时出错: {e}")
        raise

@mcp.tool()
def analyze_excel_data(
    filepath: str,
    sheet_name: str,
    data_range: str = None,
    analysis_type: str = "summary"
) -> str:
    """对Excel数据进行统计分析，支持基本统计、相关性分析等。
    
    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        data_range: 数据范围，如"A1:C10"，默认为整个表格
        analysis_type: 分析类型，可选值为"summary", "correlation", "histogram", "pivot"
    """
    try:
        full_path = get_excel_path(filepath)
        import pandas as pd
        import numpy as np
        import json
        
        # 读取Excel数据
        if data_range:
            from excel_mcp.data import read_excel_range
            data = read_excel_range(full_path, sheet_name, data_range.split(':')[0], data_range.split(':')[1] if ':' in data_range else None)
            df = pd.DataFrame(data)
        else:
            df = pd.read_excel(full_path, sheet_name=sheet_name)
        
        # 根据分析类型执行不同的分析
        if analysis_type == "summary":
            # 基本统计分析
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                return "没有找到用于汇总分析的数值列"
                
            summary = df[numeric_cols].describe().to_dict()
            return json.dumps(summary, indent=2)
            
        elif analysis_type == "correlation":
            # 相关性分析
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) < 2:
                return "相关性分析至少需要2个数值列"
                
            corr = df[numeric_cols].corr().to_dict()
            return json.dumps(corr, indent=2)
            
        elif analysis_type == "histogram":
            # 直方图分析 (返回数据的分布情况)
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) == 0:
                return "没有找到用于直方图分析的数值列"
                
            histograms = {}
            for col in numeric_cols:
                hist, bin_edges = np.histogram(df[col].dropna(), bins=10)
                histograms[str(col)] = {
                    "counts": hist.tolist(),
                    "bin_edges": bin_edges.tolist(),
                    "min": df[col].min(),
                    "max": df[col].max(),
                    "mean": df[col].mean(),
                    "median": df[col].median()
                }
            return json.dumps(histograms, indent=2)
            
        elif analysis_type == "pivot":
            # 简单数据透视分析
            if len(df.columns) < 2:
                return "数据透视分析至少需要2列数据"
                
            # 找出分类列和数值列
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
            category_cols = [c for c in df.columns if c not in numeric_cols]
            
            if not numeric_cols or not category_cols:
                return "数据透视分析同时需要数值列和类别列"
                
            # 使用第一个分类列和第一个数值列创建简单透视表
            pivot = pd.pivot_table(
                df, 
                values=numeric_cols[0],
                index=category_cols[0],
                aggfunc=['count', 'mean', 'sum', 'min', 'max']
            ).to_dict()
            
            return json.dumps(pivot, indent=2)
        else:
            return f"不支持的分析类型: {analysis_type}"
    
    except Exception as e:
        logger.error(f"分析数据时出错: {e}")
        return f"错误: {str(e)}"

@mcp.tool()
def batch_apply_excel_formulas(
    filepath: str,
    sheet_name: str,
    formulas: List[Dict[str, str]]
) -> str:
    """批量应用多个公式到不同单元格，提高性能。
    
    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        formulas: 公式列表，每个元素是包含cell和formula键的字典
    """
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.calculations import batch_apply_formulas
        result = batch_apply_formulas(full_path, sheet_name, formulas)
        return f"成功应用了 {result['applied_count']} 个公式，{result['failed_count']} 个失败"
    except (ValidationError, CalculationError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"批量应用公式时出错: {e}")
        raise

@mcp.tool()
def apply_array_excel_formula(
    filepath: str,
    sheet_name: str,
    start_cell: str,
    end_cell: str,
    formula: str
) -> str:
    """将数组公式应用到单元格区域。
    
    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        start_cell: 开始单元格
        end_cell: 结束单元格
        formula: 数组公式
    """
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.calculations import apply_array_formula
        result = apply_array_formula(full_path, sheet_name, start_cell, end_cell, formula)
        return result["message"]
    except (ValidationError, CalculationError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"应用数组公式时出错: {e}")
        raise

@mcp.tool()
def calculate_with_pandas_excel(
    filepath: str,
    sheet_name: str,
    range_ref: str,
    operation: str,
    output_cell: str = None
) -> str:
    """使用pandas进行高性能计算，支持sum, mean, max, min, count, corr等操作。
    
    Args:
        filepath: Excel文件路径
        sheet_name: 工作表名称
        range_ref: 数据范围引用，如"A1:C10"
        operation: 执行的操作，如"sum", "mean", "max", "min", "count", "corr"
        output_cell: 将结果输出到的单元格，可选
    """
    try:
        full_path = get_excel_path(filepath)
        from excel_mcp.calculations import calculate_with_pandas
        result = calculate_with_pandas(full_path, sheet_name, range_ref, operation, output_cell)
        
        if isinstance(result["result"], dict):
            # 对于字典结果（如相关性），格式化输出
            import json
            result_str = json.dumps(result["result"], indent=2)
            return f"在范围 {range_ref} 上计算 {operation} 的结果:\n{result_str}"
        else:
            # 对于单个值结果
            return f"在范围 {range_ref} 上计算 {operation} 的结果: {result['result']}"
    except (ValidationError, CalculationError) as e:
        return f"错误: {str(e)}"
    except Exception as e:
        logger.error(f"使用pandas计算时出错: {e}")
        raise

@mcp.tool()
def process_excel_from_url(url: str, operation: str, operation_params: Dict[str, Any]) -> str:
    """从URL下载Excel文件，使用指定操作处理，并上传回服务器。
    
    Args:
        url: 要下载和处理的Excel文件的URL
        operation: 要执行的操作名称（如'format_range', 'apply_formula'）
        operation_params: 包含指定操作参数的字典
        
    Returns:
        带有处理后文件URL的结果消息
    """
    try:
        # 生成临时文件路径
        import tempfile
        import uuid
        temp_dir = tempfile.gettempdir()
        temp_filename = f"temp_excel_{uuid.uuid4()}.xlsx"
        temp_filepath = os.path.join(temp_dir, temp_filename)
        
        # 下载文件
        from excel_mcp.workbook import download_file_from_url
        download_file_from_url(url, temp_filepath)
        logger.info(f"Downloaded file from {url} to {temp_filepath}")
        
        # 获取文件名用于上传后的文件名
        original_filename = os.path.basename(url.split('/')[-1])
        processed_filename = f"processed_{original_filename}"
        processed_filepath = os.path.join(EXCEL_FILES_PATH, processed_filename)
        
        # 复制文件到Excel文件目录
        import shutil
        shutil.copy2(temp_filepath, processed_filepath)
        
        # 打印operation_params
        logger.info(f"operation_params: {operation_params}")
        
        # 确保operation_params是字典类型
        if isinstance(operation_params, str):
            import json
            try:
                operation_params = json.loads(operation_params)
            except json.JSONDecodeError:
                logger.error(f"Invalid JSON in operation_params: {operation_params}")
                raise ValueError(f"Invalid JSON format in operation_params")
        
        # 执行指定操作
        result_message = ""
        if operation == "format_range":
            # 替换filepath参数为处理后的文件路径
            params = {k: v for k, v in operation_params.items() if k != "filepath"}
            result_message = format_range(processed_filepath, **params)
        elif operation == "apply_formula":
            params = {k: v for k, v in operation_params.items() if k != "filepath"}
            result_message = apply_formula(processed_filepath, **params)
        elif operation == "write_data_to_excel":
            params = {k: v for k, v in operation_params.items() if k != "filepath"}
            result_message = write_data_to_excel(processed_filepath, **params)
        elif operation == "create_chart":
            params = {k: v for k, v in operation_params.items() if k != "filepath"}
            result_message = create_chart(processed_filepath, **params)
        elif operation == "create_pivot_table":
            params = {k: v for k, v in operation_params.items() if k != "filepath"}
            result_message = create_pivot_table(processed_filepath, **params)
        else:
            raise ValueError(f"Unsupported operation: {operation}")
        
        # 上传处理后的文件
        from excel_mcp.workbook import upload_file_to_server
        upload_result = upload_file_to_server(processed_filepath)
        
        # 清理临时文件
        os.remove(temp_filepath)
        
        return f"操作 '{operation}' 成功完成。处理后的文件可在以下位置获取: {upload_result['file_url']}\n操作结果: {result_message}"
    except Exception as e:
        logger.error(f"从URL处理Excel时出错: {e}")
        raise WorkbookError(f"从URL处理Excel失败: {str(e)}")

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