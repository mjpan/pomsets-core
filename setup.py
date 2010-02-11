from setuptools import setup, find_packages

setup(
    name='pomsets-core',
    version='0.1.0',
    packages=find_packages(
        'src',exclude=["*.test", "*.test.*", "test.*", "test"]),
    package_dir={'':'src'},

    test_suite="test",
    install_requires = [
        'cloudpool>=0.1.0',
        'currypy>=0.1.0',
        'pypatterns>=0.1.0',
        'Reaction>=0.2',
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
