from setuptools import setup, find_packages
from distutils.util import convert_path

with open("README.md", 'r') as f:
    long_description = f.read()

main_ns = {}
ver_path = convert_path('hyperstream/version.py')
with open(ver_path) as ver_file:
    exec(ver_file.read(), main_ns)

with open('requirements.txt') as f:
    required = f.read().splitlines()

description = (
    'Hyperstream is a large-scale, '
    'flexible and robust software '
    'package for processing streaming data'
)

authors = [
    'Tom Diethe',
    'Meelis Kull', 
    'Niall Twomey',
    'Kacper Sokol',
    'Hao Song',
    'Emma Tonkin',
    'Peter Flach'
]

packages = find_packages()

setup(
   name='hyperstream',
   version=main_ns['__version__'],
   description=description,
   license="MIT",
   long_description=long_description,
   author='; '.join(authors),
   author_email='hyperstreamhq@googlegroups.com',
   url="https://irc-sphere.github.io/HyperStream/",
   packages=packages,
   install_requires=required,
   scripts=[]
)
