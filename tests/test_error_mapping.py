"""
Test error mapping and CLI exit code functionality.

Validates that exceptions are correctly mapped to exit codes and that
the run_and_exit wrapper handles errors appropriately for CLI commands.
"""
from __future__ import annotations

from unittest.mock import Mock

import pytest
import typer

from modelops_bundles.operations.mappers import exit_code_for, run_and_exit, EXIT_CODES


class TestExitCodeMapping:
    """Test exception to exit code mapping."""
    
    def test_known_exceptions_mapped_correctly(self):
        """Test that known exceptions map to correct exit codes."""
        # Create mock exceptions with specific class names
        bundle_not_found = Mock()
        bundle_not_found.__class__.__name__ = "BundleNotFoundError"
        
        validation_error = Mock()
        validation_error.__class__.__name__ = "ValidationError"
        
        download_error = Mock()
        download_error.__class__.__name__ = "BundleDownloadError"
        
        
        role_mismatch = Mock()
        role_mismatch.__class__.__name__ = "RoleLayerMismatch"
        
        workdir_conflict = Mock()
        workdir_conflict.__class__.__name__ = "WorkdirConflict"
        
        # Test mappings
        assert exit_code_for(bundle_not_found) == 1
        assert exit_code_for(validation_error) == 2
        assert exit_code_for(download_error) == 3
        assert exit_code_for(role_mismatch) == 11
        assert exit_code_for(workdir_conflict) == 12

    def test_unknown_exception_maps_to_fallback(self):
        """Test that unknown exceptions map to fallback exit code."""
        unknown_error = Mock()
        unknown_error.__class__.__name__ = "SomeUnknownError"
        
        assert exit_code_for(unknown_error) == 3  # Fallback code

    def test_standard_exceptions_use_fallback(self):
        """Test that standard Python exceptions use fallback code."""
        assert exit_code_for(ValueError("test")) == 2  # ValueError maps to validation error
        assert exit_code_for(RuntimeError("test")) == 3
        assert exit_code_for(FileNotFoundError("test")) == 3
        assert exit_code_for(PermissionError("test")) == 3

    def test_exit_code_constants(self):
        """Test that EXIT_CODES constants match specification."""
        assert EXIT_CODES["BundleNotFoundError"] == 1
        assert EXIT_CODES["ValidationError"] == 2
        assert EXIT_CODES["BundleDownloadError"] == 3
        assert EXIT_CODES["RoleLayerMismatch"] == 11
        assert EXIT_CODES["WorkdirConflict"] == 12

    def test_exit_code_completeness(self):
        """Test that all specified error types are mapped."""
        expected_errors = {
            "BundleNotFoundError",
            "ValidationError",
            "ValueError",  # Added ValueError mapping
            "BundleDownloadError",
            "RoleLayerMismatch",
            "WorkdirConflict"
        }
        
        assert set(EXIT_CODES.keys()) == expected_errors


class TestRunAndExit:
    """Test run_and_exit wrapper functionality."""
    
    def test_successful_function_returns_result(self):
        """Test that successful function execution returns result."""
        def success_func():
            return "success result"
        
        result = run_and_exit(success_func)
        assert result == "success result"

    def test_function_exception_raises_typer_exit(self):
        """Test that function exceptions are converted to typer.Exit."""
        def failing_func():
            # Create a proper exception that can be raised
            class BundleNotFoundError(Exception):
                pass
            raise BundleNotFoundError("test error")
        
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(failing_func)
        
        assert exc_info.value.exit_code == 1

    def test_exception_chaining_preserved(self):
        """Test that original exception is preserved as cause."""
        original_error = ValueError("original error")
        
        def failing_func():
            raise original_error
        
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(failing_func)
        
        assert exc_info.value.__cause__ is original_error

    def test_different_exceptions_map_to_different_codes(self):
        """Test that different exceptions produce different exit codes."""
        def bundle_not_found_func():
            class BundleNotFoundError(Exception):
                pass
            raise BundleNotFoundError("test error")
        
        def validation_error_func():
            class ValidationError(Exception):
                pass
            raise ValidationError("test error")
        
        # Test BundleNotFoundError -> exit code 1
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(bundle_not_found_func)
        assert exc_info.value.exit_code == 1
        
        # Test ValidationError -> exit code 2
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(validation_error_func)
        assert exc_info.value.exit_code == 2

    def test_nested_exceptions_use_outer_type(self):
        """Test that nested exceptions use the outer exception type."""
        def nested_func():
            try:
                raise ValueError("inner error")
            except ValueError as e:
                class BundleDownloadError(Exception):
                    pass
                raise BundleDownloadError("test error") from e
        
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(nested_func)
        
        assert exc_info.value.exit_code == 3  # BundleDownloadError code

    def test_lambda_functions_supported(self):
        """Test that lambda functions work with run_and_exit."""
        result = run_and_exit(lambda: "lambda result")
        assert result == "lambda result"
        
        with pytest.raises(typer.Exit):
            run_and_exit(lambda: (_ for _ in ()).throw(RuntimeError("lambda error")))

    def test_function_with_arguments_not_supported_directly(self):
        """Test that run_and_exit expects zero-argument callables."""
        def func_with_args(arg1, arg2):
            return arg1 + arg2
        
        # Should work with lambda wrapper
        result = run_and_exit(lambda: func_with_args(1, 2))
        assert result == 3

    def test_exit_code_consistency_across_calls(self):
        """Test that same exception type always produces same exit code."""
        def validation_error_func():
            class ValidationError(Exception):
                pass
            raise ValidationError("test error")
        
        # Call multiple times and verify consistent exit codes
        exit_codes = []
        for _ in range(3):
            with pytest.raises(typer.Exit) as exc_info:
                run_and_exit(validation_error_func)
            exit_codes.append(exc_info.value.exit_code)
        
        assert all(code == 2 for code in exit_codes)


class TestErrorMappingIntegration:
    """Test error mapping integration scenarios."""
    
    def test_cli_command_simulation(self):
        """Test simulation of CLI command error handling."""
        def simulate_cli_resolve():
            # Simulate bundle not found during resolve
            class BundleNotFoundError(Exception):
                pass
            raise BundleNotFoundError("test error")
        
        def simulate_cli_materialize():
            # Simulate validation error during materialize
            class ValidationError(Exception):
                pass
            raise ValidationError("test error")
        
        # Test resolve command error
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(simulate_cli_resolve)
        assert exc_info.value.exit_code == 1
        
        # Test materialize command error  
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(simulate_cli_materialize)
        assert exc_info.value.exit_code == 2

    def test_error_boundary_isolation(self):
        """Test that errors don't leak between command invocations."""
        def first_command():
            class BundleNotFoundError(Exception):
                pass
            raise BundleNotFoundError("test error")
        
        def second_command():
            return "success"
        
        # First command fails
        with pytest.raises(typer.Exit) as exc_info:
            run_and_exit(first_command)
        assert exc_info.value.exit_code == 1
        
        # Second command succeeds independently  
        result = run_and_exit(second_command)
        assert result == "success"

    def test_specification_compliance(self):
        """Test compliance with exit code specification."""
        # Exit codes from specification §9
        expected_mappings = {
            "BundleNotFoundError": 1,      # Bundle not found
            "ValidationError": 2,          # Validation error
            "BundleDownloadError": 3,      # Network/download error
            "RoleLayerMismatch": 11,       # Role/layer mismatch
            "WorkdirConflict": 12,         # Workdir conflict
        }
        
        for error_name, expected_code in expected_mappings.items():
            exc = Mock()
            exc.__class__.__name__ = error_name
            assert exit_code_for(exc) == expected_code