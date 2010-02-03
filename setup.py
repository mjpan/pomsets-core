from setuptools import setup, find_packages

setup(
    name='pomsets',
    version='0.1',
    packages=find_packages(
        'src',exclude=["*.test", "*.test.*", "test.*", "test"]),
    package_dir={'':'src'}
)
