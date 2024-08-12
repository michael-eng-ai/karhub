import unittest
import sys
import os

# Adiciona o diretório src ao PYTHONPATH
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'src')))

# Descobre e carrega todos os testes
loader = unittest.TestLoader()
start_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), 'tests'))
suite = loader.discover(start_dir, pattern='test_*.py')

# Executa os testes
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(suite)

# Sai com um código de status não-zero se houver falhas nos testes
sys.exit(not result.wasSuccessful())