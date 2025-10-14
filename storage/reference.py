def reconstruct_type_reference(obj):
    """Reconstruct type references like INT, VARCHAR"""
    type_name = obj['__name__']
    module_name = obj.get('__module__', 'builtins')
    
    try:
        if module_name == 'builtins':
            return getattr(__builtins__, type_name, str)
        else:
            # Check datatypes module
            if 'datatypes' in globals():
                datatypes_dict = globals()['datatypes']
                if hasattr(datatypes_dict, type_name):
                    return getattr(datatypes_dict, type_name)
                elif isinstance(datatypes_dict, dict) and type_name in datatypes_dict:
                    return datatypes_dict[type_name]
            
            # Try importing the module
            module = __import__(module_name, fromlist=[type_name])
            return getattr(module, type_name, str)
    except (ImportError, AttributeError):
        print(f"Warning: Could not find type {type_name}")
        return str