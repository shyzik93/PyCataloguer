import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='pycataloguer',
    version='0.1.0',
    license='LGPL-3.0',
    description='Cataloguer for your files',
    packages=['pycataloguer'],
    long_description=open(os.path.join(os.path.dirname(__file__), 'README.md')).read(),
    author='Konstantin Polyakov',
    author_email='shyzik93@mail.ru',
    url='https://github.com/shyzik93/PyCataloguer',
    download_url='https://github.com/shyzik93/pycataloguer/archive/master.zip',

    install_requires=['SQLAlchemy'],

    entry_points={
        'console_scripts': ['pycat = pycataloguer.cli:do_cmd']
    },
    include_package_data=True,
)

'''
# uninstall globally
sudo python setup.py install --record installed.txt
sudo xargs rm -vr < installed.txt

# uinstall in your home directory
python setup.py install --user --record installed.txt
xargs rm -vr < installed.txt
'''