from setuptools import setup


setup(name='dock',
      version='0.1.1',
      description='A library for moving data between a file system and a data store. '
                  'Based on Python, Git, and good intentions.',
      url='https://github.com/pwalsh/dock',
      author='Paul Walsh',
      author_email='paulywalsh@gmail.com',
      license='BSD',
      packages=['dock', 'dock.incoming', 'dock.outgoing', 'fabfile'],
      zip_safe=False)
