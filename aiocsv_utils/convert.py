import typing as _typing


def convert_str(value: _typing.Any) -> float | int | bool | str:
    """Tries to coerce a value to float, int, bool and finally str.
    
    Parameters
    ----------
    value : Any
        Value to be coerced into float, int, or bool.
        
    Returns
    -------
    float | int | bool | str
        Returns coerced value if successful, or str value if it fails to coerce.
    """
    value = str(value)
    if value.count('.') == 1:
        try:
            return float(value)
        except ValueError:
            pass
    try:
        return int(value)
    except ValueError:
        pass
    if value == 'True':
        return True
    if value == 'False':
        return False
    return value