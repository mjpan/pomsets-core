from setuptools import setup, find_packages

setup(
    name='pomsets',
    version='0.1',
    packages=find_packages(
        'src',exclude=["*.test", "*.test.*", "test.*", "test"]),
    package_dir={'':'src'},

    test_suite="test",
    install_requires = [
        'currypy>=0.1.0',
        'Reaction>=0.2',
        'pypatterns>=0.1.0',
        ],

    # metadata for upload to PyPI
    author = "michael j pan",
    author_email = "mikepan@gmail.com",
    description = "generic patterns",
    license = "New BSD",
    keywords = "object relational command filter",
    url = "http://code.google.com/p/pypatterns/",
    long_description = "This package implements some common patterns in Python",
    platforms = ["All"]

)
