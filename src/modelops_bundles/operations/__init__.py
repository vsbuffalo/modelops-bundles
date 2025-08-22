"""
Operations package - Application service layer between CLI and runtime.

This package provides the Operations facade that orchestrates CLI commands,
centralizes error mapping, and handles output formatting while keeping
CLI commands thin and testable.
"""
from .facade import Operations, OpsConfig
from .mappers import exit_code_for, run_and_exit

__all__ = ["Operations", "OpsConfig", "exit_code_for", "run_and_exit"]