from setuptools import setup

setup(
    name='kashpy',
    version='0.0.1',    
    description='A Kafka Shell based on Python',
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
