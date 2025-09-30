from setuptools import setup, find_packages

setup(
    name="snow-depth-sensors",
    version="1.0.0",
    description="CLI app to fetch snow depth sensor data from South Tyrol weather service",
    packages=find_packages(),
    install_requires=[
        "pandas>=1.5.0",
        "requests>=2.25.0", 
        "click>=8.0.0",
    ],
    entry_points={
        "console_scripts": [
            "snow-sensors=snow_sensors.cli:main",
        ],
    },
    python_requires=">=3.8",
)