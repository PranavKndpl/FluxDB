from setuptools import setup, find_packages

setup(
    name="fluxdb-driver",
    version="1.0.0",
    description="Official Python driver for the FluxDB C++ Database",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Pranav Kandpal",
    url="https://github.com/your-username/FluxDB",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)