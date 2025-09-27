from setuptools import setup, find_packages


setup(
    name='qtask_nano', 
    version='0.0.1', 
    packages=find_packages(),
    description='A simple Python task queue',
    install_requires=['redis'],
    entry_points={
        'console_scripts': [
        ],
    },
    python_requires='>=3',
    include_package_data=True,
    author='Liu Shengli',
    url='http://github.com/gseismic/qtask_nano',
    zip_safe=False,
    author_email='liushengli203@163.com'
)

