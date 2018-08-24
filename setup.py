import os.path
import re
import sys

from setuptools import find_packages, setup

install_requires = ['aiohttp']


assert sys.version_info >= (3, 5), 'asyncnsq requires Python 3.5 or later'


def read(*parts):
    path = os.path.abspath(__file__)
    dir_path = os.path.dirname(path)    
    with open(os.path.join(dir_path, *parts), 'rt') as f:
        return f.read().strip()


def read_version():
    regexp = re.compile(r"^__version__\W*=\W*'([\d.abrc]+)'")
    init_py = os.path.join(os.path.dirname(__file__),
                           'asyncnsq', '__init__.py')
    with open(init_py) as f:
        for line in f:
            match = regexp.match(line)
            if match is not None:
                return match.group(1)
        raise RuntimeError('Cannot find version in asyncnsq/__init__.py')


classifiers = [
    'License :: OSI Approved :: MIT License',
    'Development Status :: 4 - Beta',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Operating System :: POSIX',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Software Development',
    'Topic :: Software Development :: Libraries',
]

setup(name='asyncnsq',
      version=read_version(),
      description=("asyncio async/await nsq support"),
      long_description=read('README.md'),
      classifiers=classifiers,
      platforms=["POSIX"],
      author="aohan237",
      author_email="aohan237@gmail.com",
      url="https://github.com/aohan237/asyncnsq",
      license="MIT",
      packages=find_packages(exclude=["tests"]),
      install_requires=install_requires,
      extras_require={"snappy": ["python-snappy"]},
      include_package_data=True)
