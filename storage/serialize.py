from datetime import datetime, date, time

def deep_serialize(obj):
    """
    Recursively convert ALL objects to msgpack-compatible types.
    This ensures no custom objects slip through.
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, (list, tuple)):
        return [deep_serialize(item) for item in obj]
    elif isinstance(obj, dict):
        return {str(key): deep_serialize(value) for key, value in obj.items()}
    elif isinstance(obj, set):
        return list(deep_serialize(item) for item in obj)
    elif isinstance(obj, type):
        return {
            '__type__': 'type_reference',
            '__name__': obj.__name__,
            '__module__': getattr(obj, '__module__', 'builtins')
        }
    elif isinstance(obj, (datetime, date, time)):
        return {
            '__type__': 'datetime_obj',
            '__class__': obj.__class__.__name__,
            '__value__': obj.isoformat()
        }
    elif callable(obj):
        return {
            '__type__': 'callable_fallback',
            '__value__': str(obj),
            '__name__': getattr(obj, '__name__', 'unknown')
        }
    elif hasattr(obj, '__dict__') or hasattr(obj.__class__, '__slots__'):
        # Handle all custom objects (AST nodes, SQL types, etc.)
        return {
            '__type__': 'ast_object',
            '__class__': obj.__class__.__name__,
            '__module__': obj.__class__.__module__,
            '__slots__': getattr(obj.__class__, '__slots__', None),
            '__data__': deep_serialize_object_data(obj)
        }
    else:
        # Last resort: convert to string
        return {
            '__type__': 'string_fallback',
            '__value__': str(obj)
        }
        
        
def deep_serialize_object_data(obj):
    """Extract and deeply serialize all attributes from an object"""
    data = {}
    
    # Handle __slots__ classes
    if hasattr(obj.__class__, '__slots__'):
        for slot in obj.__class__.__slots__:
            if hasattr(obj, slot):
                value = getattr(obj, slot)
                data[slot] = deep_serialize(value)
    
    # Handle regular classes with __dict__
    if hasattr(obj, '__dict__'):
        for key, value in obj.__dict__.items():
            data[key] = deep_serialize(value)
    
    return data