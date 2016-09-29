from setuptools import setup

setup(name='nbconvert_watch',
      version='0.1.2',
      description='Monitor Jupyter notebooks and run them automatically',
      url='https://github.com/APLmath/nbconvert_watch',
      author='Andrew Lee',
      author_email='andrew.lee.cal@gmail.com',
      license='MIT',
      packages=['nbconvert_watch'],
      install_requires=[
          'watchdog',
      ],
      entry_points = {
          'console_scripts': ['nbconvert-watch=nbconvert_watch.command_line:main'],
      },
      zip_safe=False)