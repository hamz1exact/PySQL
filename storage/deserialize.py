from datetime import datetime, date, time
from storage.reference import *
from storage.classes import *
def deep_deserialize(obj):
    """
    Recursively reconstruct objects from serialized data
    """
    if obj is None:
        return None
    elif isinstance(obj, (str, int, float, bool)):
        return obj
    elif isinstance(obj, list):
        return [deep_deserialize(item) for item in obj]
    elif isinstance(obj, dict):
        if '__type__' in obj:
            return deserialize_typed_object(obj)
        else:
            # Regular dictionary
            return {key: deep_deserialize(value) for key, value in obj.items()}
    else:
        return obj

def deserialize_typed_object(obj):
    """Deserialize objects with type information"""
    obj_type = obj['__type__']
    
    if obj_type == 'ast_object':
        return reconstruct_ast_object(obj)
    elif obj_type == 'type_reference':
        return reconstruct_type_reference(obj)
    elif obj_type == 'datetime_obj':
        return reconstruct_datetime_object(obj)
    elif obj_type == 'callable_fallback':
        return obj['__value__']
    elif obj_type == 'string_fallback':
        return obj['__value__']
    else:
        return obj
    
    
def reconstruct_ast_object(obj):
    """Reconstruct AST objects like SelectStatement, ColumnExpression"""
    class_name = obj['__class__']
    module_name = obj.get('__module__')
    data = obj['__data__']
    slots = obj.get('__slots__')
    
    # Recursively deserialize the data first
    deserialized_data = deep_deserialize(data)
    
    # Find the class
    cls = find_class(class_name, module_name)
    
    if cls:
        try:
            # Create instance
            instance = cls.__new__(cls)
            
            # Set attributes
            if slots:
                for slot in slots:
                    if slot in deserialized_data:
                        setattr(instance, slot, deserialized_data[slot])
            else:
                for key, value in deserialized_data.items():
                    setattr(instance, key, value)
            
            return instance
            
        except Exception as e:
            print(f"Warning: Could not reconstruct {class_name}: {e}")
            return deserialized_data
    
    return deserialized_data

def reconstruct_datetime_object(obj):
    """Reconstruct datetime objects"""
    class_name = obj['__class__']
    iso_value = obj['__value__']
    
    try:
        if class_name == 'datetime':
            return datetime.fromisoformat(iso_value)
        elif class_name == 'date':
            return datetime.fromisoformat(iso_value).date()
        elif class_name == 'time':
            return datetime.fromisoformat(f"1970-01-01T{iso_value}").time()
    except ValueError:
        print(f"Warning: Could not parse datetime {iso_value}")
        return iso_value