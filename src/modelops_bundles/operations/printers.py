"""
Human-readable output formatting.

Centralizes all CLI output formatting to enable easy addition of JSON mode
in future stages while keeping CLI commands thin and focused.
"""
from __future__ import annotations

import typer
from typing import List, Optional
from modelops_contracts.artifacts import ResolvedBundle

# Optional Rich support for enhanced output
try:
    from rich.console import Console
    from rich.table import Table
    from rich.text import Text
    _RICH = True
    _console = Console()
except ImportError:
    _RICH = False
    _console = None

def print_resolved_bundle(bundle: ResolvedBundle, verbose: bool = False) -> None:
    """
    Print resolved bundle information in human-readable format.
    
    Shows manifest digest and available roles for the bundle.
    
    Args:
        bundle: Resolved bundle to display
        verbose: Show detailed information including media type decisions
    """
    # Format bundle label defensively
    label = (
        f"{bundle.ref.name}:{bundle.ref.version}"
        if bundle.ref.name and bundle.ref.version
        else bundle.manifest_digest[:18] + "…"
    )
    
    if _RICH:
        _console.print(f"[bold]Bundle:[/] {label}")
        _console.print(f"[bold]Manifest:[/] [dim]{bundle.manifest_digest}[/]")
        _console.print(f"[bold]Size:[/] {_format_bytes(bundle.total_size)}")
        
        if verbose:
            _console.print(f"[bold]Reference type:[/] {'digest-only' if bundle.ref.digest else 'name:version'}")
            if bundle.external_index_present:
                _console.print("[bold]External index:[/] present")
            else:
                _console.print("[bold]External index:[/] not present")
        
        if bundle.roles:
            table = Table(title="Roles")
            table.add_column("Role", style="cyan")
            table.add_column("Layers", style="yellow")
            
            for role_name, layers in sorted(bundle.roles.items()):
                layer_list = ", ".join(layers)
                table.add_row(role_name, layer_list)
            
            _console.print(table)
        else:
            _console.print("[dim]No roles defined[/]")
        return
    
    # Fallback to plain text
    typer.echo(f"Manifest: {bundle.manifest_digest}")
    typer.echo(f"Bundle: {label}")
    typer.echo(f"Size: {_format_bytes(bundle.total_size)}")
    
    if verbose:
        typer.echo(f"Reference type: {'digest-only' if bundle.ref.digest else 'name:version'}")
        typer.echo(f"External index: {'present' if bundle.external_index_present else 'not present'}")
    
    if bundle.roles:
        typer.echo("Roles:")
        for role_name, layers in sorted(bundle.roles.items()):
            layer_list = ", ".join(layers)
            typer.echo(f"  {role_name}: [{layer_list}]")
    else:
        typer.echo("No roles defined")

def print_materialize_summary(bundle: ResolvedBundle, dest: str, role: str) -> None:
    """
    Print materialization summary.
    
    Args:
        bundle: Bundle that was materialized
        dest: Destination directory
        role: Role that was materialized
    """
    # Format bundle label defensively
    label = (
        f"{bundle.ref.name}:{bundle.ref.version}"
        if bundle.ref.name and bundle.ref.version
        else bundle.manifest_digest[:18] + "…"
    )
    
    typer.echo(f"Materialized {label} to {dest}")
    typer.echo(f"Role: {role}")
    
    # Guard against unknown/missing roles
    if role in bundle.roles:
        layers = bundle.roles[role]
        typer.echo(f"Layers: {', '.join(layers)}")
    elif role == "unknown":
        if bundle.roles:
            available_roles = list(bundle.roles.keys())
            typer.echo(f"Available roles: {', '.join(available_roles)}")
            # Show the first available role as the chosen one
            first_role = available_roles[0]
            typer.echo(f"Using first available role: {first_role}")
        else:
            typer.echo("No roles defined in bundle")
    else:
        typer.echo(f"Role '{role}' not found in bundle")
        if bundle.roles:
            available_roles = list(bundle.roles.keys())
            typer.echo(f"Available roles: {', '.join(available_roles)}")

def print_export_summary(src_dir: str, out_path: str, include_external: bool) -> None:
    """
    Print export operation summary.
    
    Args:
        src_dir: Source directory that was exported
        out_path: Output archive path
        include_external: Whether external data was included
    """
    typer.echo(f"Exported {src_dir} to {out_path}")
    if include_external:
        typer.echo("External data: included")
    else:
        typer.echo("External data: pointer files only")

def print_materialize_progress(path: str, action: str, ci_mode: bool = False) -> None:
    """
    Print per-file materialization progress.
    
    Args:
        path: File path being processed
        action: Action taken (CREATED, UNCHANGED, REPLACED, CONFLICT)
        ci_mode: Whether running in CI (suppresses progress)
    """
    if not ci_mode:
        typer.echo(f"{action}: {path}")

def print_conflicts(conflicts: List[dict], max_display: int = 5) -> None:
    """
    Print workdir conflicts in human-readable format.
    
    Args:
        conflicts: List of conflict dictionaries
        max_display: Maximum number of conflicts to display
    """
    if _RICH:
        table = Table(title=f"Conflicts ({len(conflicts)})")
        table.add_column("Path", style="red")
        table.add_column("Detail", style="yellow")
        
        for conflict in conflicts[:max_display]:
            path = conflict.get("path", "unknown")
            if "expected_sha256" in conflict and "actual_sha256" in conflict:
                expected = conflict["expected_sha256"][:8]
                actual = conflict["actual_sha256"][:8]
                detail = f"expected {expected}..., got {actual}..."
            elif "error" in conflict:
                detail = conflict["error"]
            else:
                detail = "conflict"
            table.add_row(path, detail)
        
        _console.print(table)
        if len(conflicts) > max_display:
            _console.print(f"[dim]… and {len(conflicts) - max_display} more[/]")
        return
    
    # Fallback to plain text
    typer.echo(f"Found {len(conflicts)} conflicts:")
    
    for i, conflict in enumerate(conflicts[:max_display]):
        path = conflict.get("path", "unknown")
        if "expected_sha256" in conflict and "actual_sha256" in conflict:
            typer.echo(f"  {path}: content mismatch")
        elif "error" in conflict:
            typer.echo(f"  {path}: {conflict['error']}")
        else:
            typer.echo(f"  {path}: conflict")
    
    if len(conflicts) > max_display:
        typer.echo(f"  ... and {len(conflicts) - max_display} more")

def print_stub_message(command: str) -> None:
    """
    Print placeholder message for stubbed commands.
    
    Args:
        command: Command name that is stubbed
    """
    typer.echo(f"[{command}] Command implemented as stub")
    typer.echo("Full implementation coming soon")

def _format_bytes(size_bytes: int) -> str:
    """
    Format byte count as human-readable string.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted string (e.g., "1.5 MB", "42 KB")
    """
    if size_bytes == 0:
        return "0 B"
    elif size_bytes < 1024:
        return f"{size_bytes} B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.1f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.1f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"