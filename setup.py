from setuptools import setup, find_packages
setup(
    name='INEvalidador',
    version='1.4.8',
    author='Mariajose Chinchilla Moran',
    description='Validador de datos de criterios de encuestas estilo INE.',
    long_description='',
    url='',# el repo de github (cuando se haga publico)
    keywords='development, setup, setuptools',
    python_requires='>=3.7',
    packages=find_packages(),
    py_modules=['validador'],
    install_requires=[
        'openpyxl==3.1.2',
        'pandas==2.0.3',
        'pyreadstat==1.2.2',
        'tqdm==4.65.0',
        'mysql-connector-python==8.1.0',
        'sqlalchemy==2.0.19',
        'pyarrow',
        "PyDrive==1.3.1"
    ],
    package_data={
        'INEvalidador': ['archivos/*'],
    },
    include_package_data=True,
)
