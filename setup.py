from setuptools import setup, find_packages

setup(
    name='pomsets-core',
    version='1.0.0',
    packages=find_packages(
        'src',exclude=["*.test", "*.test.*", "test.*", "test",
                       "*.utils", "*.utils.*", "utils.*", "utils",]),
    package_dir={'':'src'},
    test_suite="test",
    install_requires = [
        'cloudpool>=0.1.2',
        'pypatterns>=0.1.0',
        'Reaction>=0.2',
        'simplejson>=2.0.9'
        ],

    # metadata for upload to PyPI
    author = "michael j pan",
    author_email = "mikepan@gmail.com",
    description = "workflow management for the cloud",
    license = "GPL (personal use) or commercial",
    keywords = "workflow cloud hadoop parameter sweep",
    url = "http://pomsets.org",
    long_description = "pomsets-core implements the core functionality for workflow management in the cloud.",
    platforms = ["All"]

)
