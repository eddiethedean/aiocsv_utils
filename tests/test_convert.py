from aiocsv_utils.convert import convert_str


def test_convert_int():
    assert convert_str(1) == 1
    
def test_convert_str_int():
    assert convert_str('11') == 11
    
def test_convert_float():
    assert convert_str(1.1) == 1.1
    
def test_convert_str_float():
    assert convert_str('1.1') == 1.1
    
def test_convert_str():
    assert convert_str('NaN') == 'NaN'
    
def test_convert_true():
    assert convert_str(True) == True
    
def test_convert_str_true():
    assert convert_str('True') == True
    
def test_convert_false():
    assert convert_str(False) == False
    
def test_convert_str_false():
    assert convert_str('False') == False
    
def test_convert_list():
    assert convert_str([1, 2, 3]) == '[1, 2, 3]'
    
def test_convert_bad_float():
    assert convert_str('1.13Q') == '1.13Q'
    
def test_convert_unicode():
    assert convert_str('\u0030') == 0
    
def test_convert_negative():
    assert convert_str('-1') == -1
    
def test_covert_power():
    assert convert_str('\u00B2') == '\u00B2'