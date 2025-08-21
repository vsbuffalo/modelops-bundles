# Storage Abstractions

- Protocols in `base.py` define storage interfaces
- Fakes in `fakes/` subclass protocols for drift control  
- Fakes excluded from production wheels
- Used by providers to isolate storage concerns from runtime