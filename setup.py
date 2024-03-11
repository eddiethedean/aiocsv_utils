from setuptools import setup, find_packages

setup(
    name = 'aiocsv_utils',
    packages = find_packages(),
    install_requires = [
        'aiofiles',
        'aiocsv',
        'aioitertools'
    ]
)