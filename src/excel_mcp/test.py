

def main():

    exec_globals = globals()  # 使用当前全局命名空间
    exec_locals = {"a": "3333"}
    python_code = """
import re  # 显式导入 re 模块
import cpca
pattern = re.compile(r'\\d+')
result = pattern.findall('123 abc 456')
print(result, a)
"""
    exec(python_code, exec_globals, exec_locals)


main()
