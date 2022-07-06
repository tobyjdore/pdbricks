from setuptools import setup, find_packages

with open('README.md') as f:
    long_description = f.read()

setup(
  name = pdbricks
  packages = pdbricks
  install_requires = ['databricks-sql-connector'],
  version = '1.0',
  license='MIT',
  long_description=long_description,
  long_description_content_type='text/markdown',
  author = 'Toby Dore',
  author_email = 'toby.dore@gmail.com',
  url = '//https://github.com/tobyjdore/pdbricks',
  keywords = ['Azure', 'DevOps', 'Python', 'pandas', 'DataBricks'],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Topic :: Software Development :: Build Tools',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.9',
  ],
)