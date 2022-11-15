from setuptools import setup
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text()

setup(
    name='kashpy',
    version='0.0.9',
    description='A Kafka Shell based on Python',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/xdgrulez/kash.py',
    author='Ralph M. Debusmann',
    author_email='matthias.debusmann@gmail.com',
    license='Apache License 2.0',
    packages=['kashpy'],
    install_requires=['confluent_kafka>=1.9.0',
                      'grpcio-tools',
                      'requests',
                      'fastavro',
                      'jsonschema'
                      ],
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
