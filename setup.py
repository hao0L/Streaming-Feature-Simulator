import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="streaming_feature_simulator",
    version="0.0.1",
    author="Hao Liu",
    author_email="haoliu@squareup.com",
    description="Generate SQL scripts to simulate streaming features offline",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/hao0L/Streaming_Feature_Simulator",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    include_package_data=True,
    python_requires=">=3.6",
)