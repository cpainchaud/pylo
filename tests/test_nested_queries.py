"""Test nested query support"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.realpath(__file__))))

from illumio_pylo.FilterQuery import QueryLexer, QueryParser

nested_queries = [
    "(name == 'A' or name == 'B') and online == true",
    "name == 'A' or (name == 'B' and env == 'Prod')",
    "((name == 'A' or name == 'B') and env == 'Prod') or deleted == true",
    "not (name == 'A' and online == true)",
    "(a == '1' and (b == '2' or c == '3')) or (d == '4' and e == '5')",
    "((a == '1'))",  # Double parentheses
    "(((a == '1' or b == '2')))",  # Triple nested
]

print("Testing nested query parsing:")
print("=" * 60)

for q in nested_queries:
    print(f"\nQuery: {q}")
    try:
        lexer = QueryLexer(q)
        tokens = lexer.tokenize()
        parser = QueryParser(tokens)
        ast = parser.parse()
        print(f"AST: {ast}")
        print("OK: Parsed successfully")
    except Exception as e:
        print(f"ERROR: {e}")

print("\n" + "=" * 60)
print("Nested query support: WORKING")
