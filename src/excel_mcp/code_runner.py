import ast


def transform_top_level_imports(code_string):
    """
    将 Python 代码字符串中的所有顶层导入语句转换为 __import__ 形式，
    并处理导入时的别名。

    Args:
        code_string: 包含 Python 代码的字符串。

    Returns:
        转换后的 Python 代码字符串。
    """
    tree = ast.parse(code_string)
    new_lines = []

    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                module_name = alias.name
                asname = alias.asname
                if asname:
                    new_lines.append(f"{asname} = __import__('{module_name}')")
                else:
                    new_lines.append(f"{module_name} = __import__('{module_name}')")
        elif (
            isinstance(node, ast.ImportFrom) and node.level == 0
        ):  # 处理顶层 from ... import ...
            module_name = node.module
            if module_name:
                for alias in node.names:
                    imported_name = alias.name
                    asname = alias.asname
                    if asname:
                        new_lines.append(
                            f"{asname} = __import__('{module_name}', fromlist=['{imported_name}']).{imported_name}"
                        )
                    else:
                        new_lines.append(
                            f"{imported_name} = __import__('{module_name}', fromlist=['{imported_name}']).{imported_name}"
                        )
            else:
                # 处理 "from . import ..." 或 "from .. import ..." 等相对导入，这里我们跳过顶层相对导入
                new_lines.append(ast.unparse(node))  # 直接使用ast.unparse
        else:
            new_lines.append(ast.unparse(node))

    return "\n".join(new_lines)


def run_python_code(python_code, exec_locals):
    # exec_globals = globals()
    return exec(transform_top_level_imports(python_code), None, exec_locals)
