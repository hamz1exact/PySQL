def find_class(class_name, module_name):
    """Find a class by name, trying multiple strategies"""
    
    # Try specific known classes first
    known_classes = {
        'SelectStatement': 'sql_ast',
        'ColumnExpression': 'sql_ast', 
        'BinaryOperation': 'sql_ast',
        'Function': 'sql_ast',
        'LiteralExpression': 'sql_ast',
        'ConditionExpr': 'sql_ast',
        'OrderBy': 'sql_ast',
        'TableReference': 'sql_ast',
        'MathFunction': 'sql_ast',
        'StringFunction': 'sql_ast',
        'Cast': 'sql_ast',
        'Extract': 'sql_ast',
        'DateDIFF': 'sql_ast',
        'CaseWhen': 'sql_ast',
        'Concat': 'sql_ast',
        'Replace': 'sql_ast',
        'CoalesceFunction': 'sql_ast',
        'NullIF': 'sql_ast',
        'CurrentDate': 'sql_ast',
        'Between': 'sql_ast',
        'Membership': 'sql_ast',
        'IsNullCondition': 'sql_ast',
        'LikeCondition': 'sql_ast',
        'NegationCondition': 'sql_ast'
    }
    
    if class_name in known_classes:
        try:
            module = __import__(known_classes[class_name], fromlist=[class_name])
            return getattr(module, class_name)
        except (ImportError, AttributeError):
            pass
    
    # Try the original module
    if module_name and module_name != '__main__':
        try:
            module = __import__(module_name, fromlist=[class_name])
            return getattr(module, class_name, None)
        except (ImportError, AttributeError):
            pass
    
    # Try finding in loaded modules
    import sys
    for module in sys.modules.values():
        if module and hasattr(module, class_name):
            return getattr(module, class_name)
    
    print(f"Warning: Could not find class {class_name}")
    return None
