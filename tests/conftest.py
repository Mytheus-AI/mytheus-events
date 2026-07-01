import os
import sys

# Make the codegen helpers importable from tests (from _schemas import ...).
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), "codegen"))
