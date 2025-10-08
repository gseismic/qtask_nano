from setuptools import setup, find_packages


setup(
    name='qtask_nano', 
    version='0.2.0', 
    packages=find_packages(),
    description='A simple Python task queue',
    install_requires=['redis', 'psycopg2-binary', 'loguru', 'tenacity'],

    entry_points={
        'console_scripts': [
            'qtask_nano=qtask_nano.query_cli:main',
        ],
    },
    python_requires='>=3',
    include_package_data=True,
    author='Liu Shengli',
    url='http://github.com/gseismic/qtask_nano',
    zip_safe=False,
    author_email='liushengli203@163.com'
)

