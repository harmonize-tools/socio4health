"""Helpers to import optional dependencies lazily and provide helpful error messages."""
from importlib import import_module

OPTIONAL_EXTRAS = {
    'geopandas': 'geo',
    'matplotlib': 'viz',
    'transformers': 'ml',
    'torch': 'ml',
    'deep_translator': 'ml'
}


def import_optional(module_name: str, extra: str | None = None):
    """Attempt to import an optional module and raise a helpful error on failure.

    Parameters
    - module_name: the importable module name (e.g. 'geopandas')
    - extra: the extras_require key (e.g. 'geo') to suggest to the user

    Returns
    - the imported module
    """
    try:
        return import_module(module_name)
    except Exception as exc:  # ImportError or other import-time errors
        extra_name = extra or OPTIONAL_EXTRAS.get(module_name)
        if extra_name:
            msg = (
                f"Optional dependency '{module_name}' is required for this feature. "
                f"Install it with: pip install 'socio4health[{extra_name}]'"
            )
        else:
            msg = f"Optional dependency '{module_name}' is required for this feature. Install it manually."
        # Preserve original exception context for debugging
        raise ImportError(msg) from exc
