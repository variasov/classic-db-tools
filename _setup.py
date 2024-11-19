#!/usr/bin/env python

long_description = '''
Generate SQL Queries using a Jinja Template, without worrying about SQL Injection

JinjaSQL automatically binds parameters that are inserted into the template.
After JinjaSQL evaluates the template, you get 1) Query with placeholders
for parameters, and 2) List of values that need to be bound to the query. 

JinjaSQL doesn't actually execute the query - it only prepares the 
query and the bind parameters. You can execute the query using any 
database engine / driver you are working with.

'''

sdict = {
    'description': 'Generate SQL Queries and Corresponding Bind Parameters using a Jinja2 Template',
    'long_description': long_description,
    'url': 'https://github.com/sripathikrishnan/jinjasql',
    'download_url': 'http://cloud.github.com/downloads/sripathikrishnan/jinjasql/jinjasql-%s.tar.gz' % __version__,
    'author': 'Sripathi Krishnan',
    'author_email': 'Sripathi.Krishnan@gmail.com',
    'maintainer': 'Sripathi Krishnan',
    'maintainer_email': 'Sripathi.Krishnan@gmail.com',
    'packages': ['sql_tools'],
}

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(**sdict)

