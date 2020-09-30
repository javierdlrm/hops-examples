from setuptools import setup, find_packages

tests_require = [
    'pytest',
    'pytest-tornasync',
    'mypy'
]

setup(
    name='transformer',
    version='0.1.0',
    python_requires='>=3.6',
    packages=find_packages("transformer"),
    install_requires=[
        "kfserving>=0.2.1",
        "hsfs",
        "argparse>=1.4.0",
        "requests>=2.22.0",
        "joblib>=0.13.2",
        "pandas>=0.24.2",
        "numpy>=1.16.3",
        "kubernetes >= 9.0.0",
        "pillow==6.2.0"
    ],
    tests_require=tests_require,
    extras_require={'test': tests_require}
)