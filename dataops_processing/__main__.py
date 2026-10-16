"""
__main__.py — python -m dataops_processing entry point.

Delegates sang jobs/runner.py::main() để xử lý CLI args và dispatch job.
"""
from dataops_processing.jobs.runner import main

main()
