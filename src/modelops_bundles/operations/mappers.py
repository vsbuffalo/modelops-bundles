"""
Error mapping and CLI utilities.

Provides centralized exception-to-exit-code mapping and CLI command wrappers
to ensure consistent error handling across all Typer commands.
"""
from __future__ import annotations

import typer
from typing import Callable, Any, TypeVar

T = TypeVar('T')

# Exit code mapping per specification (§9)
EXIT_CODES = {
    "BundleNotFoundError": 1,
    "ValidationError": 2,
    "ValueError": 2,  # Add ValueError mapping
    "BundleDownloadError": 3,
    "RoleLayerMismatch": 11,
    "WorkdirConflict": 12,
}

def exit_code_for(exc: BaseException) -> int:
    """
    Map exception to standardized exit code.
    
    Returns exit codes per specification §9:
    - 0: Success
    - 1: Bundle not found (BundleNotFoundError)
    - 2: Validation error (ValidationError)  
    - 3: Network/download error (BundleDownloadError) or unknown error
    - 11: Role/layer mismatch (RoleLayerMismatch)
    - 12: Workdir conflict (WorkdirConflict)
    
    Args:
        exc: Exception to map
        
    Returns:
        Exit code (1-12, with 3 as fallback for unknown exceptions)
    """
    return EXIT_CODES.get(type(exc).__name__, 3)

def run_and_exit(func: Callable[[], T]) -> T:
    """
    Unified error wrapper for CLI commands.
    
    Executes the given function and maps any exceptions to appropriate
    exit codes using typer.Exit. This centralizes error handling so
    CLI commands don't need individual try/except blocks.
    
    Args:
        func: Function to execute
        
    Returns:
        Function result if successful
        
    Raises:
        typer.Exit: With appropriate exit code if function raises exception
    """
    try:
        return func()
    except Exception as e:
        # Special handling for WorkdirConflict to show conflict details
        if type(e).__name__ == "WorkdirConflict" and hasattr(e, 'conflicts'):
            from .printers import print_conflicts
            print_conflicts(e.conflicts)
        raise typer.Exit(code=exit_code_for(e)) from e