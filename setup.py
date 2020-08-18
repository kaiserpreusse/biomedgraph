from setuptools import setup, find_packages

setup(name='biodatagraph',
      version='0.0.1',
      description='Download data from biomedical databases and store in Neo4j.',
      url='https://github.com/kaiserpreusse/biomedgraph',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='MIT License',
      packages=find_packages(),
      install_requires=[
          'urllib3<1.25,>=1.23', 'pandas', 'xlrd', 'requests', 'ftputil',
          'psycopg2-binary', 'pronto', 'graphio', 'datacroaker'
      ],
      keywords=['NEO4J', 'Biology'],
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
