import unittest
import pandas as pd
from INEvalidador.utils import columnas_a_mayuscula, condicion_a_variables, extract_number, extraer_UPMS, concatenar_exceles

class TestFunctions(unittest.TestCase):    
    def test_columnas_a_mayuscula(self):
        df = pd.DataFrame({
            'col1': [1, 2],
            'Col2': [3, 4],
            'COL3': [5, 6]
        })
        result = columnas_a_mayuscula(df)
        expected_columns = ['COL1', 'COL2', 'COL3']
        self.assertEqual(list(result.columns), expected_columns)
    
    def test_condicion_a_variables(self):
        condicion = "VACIO ES UNA CONDICION PARA LA VARIABLE1 Y VARIABLE2 PERO NO VARIABLE3"
        result = condicion_a_variables(condicion)
        expected = ('VARIABLE1', 'VARIABLE2', 'VARIABLE3')
        self.assertEqual(result, expected)
    
    def test_extract_number(self):
        s = "random7number12345end"
        result = extract_number(s)
        self.assertEqual(result, 12345)

if __name__ == '__main__':
    unittest.main()
