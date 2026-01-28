from setuptools import setup, find_packages

setup(
    name="osint-lookup-bot",
    version="1.0.0",
    packages=find_packages(),
    install_requires=[
        "aiogram==3.10.0",
        "httpx==0.26.0",
        "python-dotenv==1.0.0",
        "aiosqlite==0.19.0",
        "aiohttp==3.9.1",
    ],
    entry_points={
        "console_scripts": [
            "osint-bot=main:main",
        ],
    },
)