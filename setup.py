from setuptools import setup

setup(
    name='gitxref',
    version='',
    packages=['gitxref'],
    url='https://github.com/ali1234/gitxref',
    license='GPLv3',
    author='Alistair Buxton',
    author_email='a.j.buxton@gmail.com',
    description='',
    install_requires=['numpy', 'tqdm', 'gitpython'],
    entry_points={
        'console_scripts': [
            'gitxref = gitxref.__main__:main'
        ]
    }
)
