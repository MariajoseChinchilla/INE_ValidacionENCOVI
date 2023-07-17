from setuptools import setup, find_packages
setup(
    name='INEvalidador',
    version='1.2.0',
    author='Luis Alfredo Alvarado Rodríguez',
    description='ETL para el informe mensual de IPC.',
    long_description='',
    url='https://github.com/1u1s4/INE_IPC',
    keywords='development, setup, setuptools',
    python_requires='>=3',
    packages=find_packages(),
    py_modules=['datosipc', 'descriptoripc', 'sqline'],
    install_requires=[
        'fredapi',
        'xlrd==2.0.1',
        'xlsxwriter',
        'pyodbc',
        'requests',
        'bs4',
        'numpy',
        'pandas',
        'pyarrow',
        'funcionesjo'
    ],
    package_data={
        'ineipc': ['IPC CA RD Y MEX.xlsx', 'imputacion.feather', 'PRECIOS_EN_PERIODO_DE_ESPERA.xlsx'],
    },
    include_package_data=True,
)