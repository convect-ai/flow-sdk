import os

from setuptools import find_packages, setup

# check if requirements.txt exists
if not os.path.exists("requirements.txt"):
    raise ValueError("requirements.txt is not found")
with open("requirements.txt", "r") as f:
    MAIN_REQUIREMENTS = f.read().splitlines()

TEST_REQUIREMENTS = [
    "pytest~=6.1",
    "pytest-mock~=3.6.1",
]



setup(
    name="convect-flow-sdk",
    version='{{VERSION_PLACEHOLDER}}',
    description="convect flow sdk",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="convect.ai",
    author_email="hi@convect.ai",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    include_package_data=True,
    package_data={
        "": [
            "*.json",
            "*.yaml",
        ]
    },
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },

    python_requires=">=3.9",
    url='https://github.com/convect-ai/flow-sdk',
    entry_points = {
    "console_scripts": ["flow-cli=convect_flow_sdk.cli:run_command"]
    },
)
