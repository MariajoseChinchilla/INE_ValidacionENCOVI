from setuptools import setup, find_packages
setup(
    name='INEvalidador',
    version='0.1',
    author='Mariajose Chinchilla Moran',
    description='Validador de datos de criterios de encuestas estilo INE.',
    long_description='',
    url='',# el repo de github (cuando se haga publico)
    keywords='development, setup, setuptools',
    python_requires='>=3.7',
    packages=find_packages(),
    py_modules=['datosipc', 'descriptoripc', 'sqline'],
    install_requires=[
        'openpyxl==3.1.2',
        'pandas==2.0.3',
        'pyreadstat==1.2.2'
    ]
)