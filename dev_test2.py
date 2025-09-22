from engine import Lexer, Parser
from datatypes import INT, VARCHAR

def test_lexer_still_works():
    """Test that lexer works after moving constants"""
    query = "SELECT id FROM users WHERE age > 25;"
    lexer = Lexer(query)
    
    # Should have tokens
    assert len(lexer.tokens) > 0
    
    # Should find SELECT token
    token_values = [token[1] for token in lexer.tokens]
    assert "SELECT" in token_values
    assert "FROM" in token_values
    print("✅ Lexer test passed!")

def test_datatypes_still_work():
    """Test that data types work after changes"""
    int_val = INT(42)
    assert int_val.value == 42
    
    varchar_val = VARCHAR("hello")
    assert varchar_val.value == "hello"
    print("✅ Data types test passed!")

if __name__ == "__main__":
    test_lexer_still_works()
    test_datatypes_still_work()
    print("✅ All tests passed!")