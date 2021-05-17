from setuptools import setup, find_packages

setup(name='biomedgraph',
      use_scm_version={
          "root": ".",
          "relative_to": __file__,
          "local_scheme": "node-and-timestamp"
      },
      setup_requires=['setuptools_scm'],
      description='Download data from biomedical databases and store in Neo4j.',
      url='https://github.com/kaiserpreusse/biomedgraph',
      author='Martin Preusse',
      author_email='martin.preusse@gmail.com',
      license='MIT License',
      packages=find_packages(),
      install_requires=[
          'urllib3', 'pandas', 'xlrd', 'requests', 'ftputil',
          'psycopg2-binary', 'pronto', 'graphio>=0.1.0', 'graphpipeline', 'lxml', 'anndata'
      ],
      keywords=['NEO4J', 'Biology'],
      zip_safe=False,
      classifiers=[
          'Programming Language :: Python',
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers'
      ],
      )
