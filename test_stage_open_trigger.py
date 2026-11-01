from pathlib import Path
import ast

source = (Path(__file__).parent / "main.py").read_text()
tree = ast.parse(source, filename="main.py")
functions = {node.name for node in ast.walk(tree) if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))}
assert "schedule_stage_open" in functions
assert "evaluate_and_execute_mint" in functions
assert "schedule_stage_open(session, slug, chain_key, next_free_start)" in source
assert "stage_open_tasks" in source
print("stage-open trigger checks passed")
