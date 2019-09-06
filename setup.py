from setuptools import setup, find_packages

setup(
    name="vertica_sql_plugin",
    version="0.1",
    packages=find_packages(),
    install_requires=["apache-airflow[vertica]"],
    author="Mikhail Akimov",
    author_email="rovinj.akimov@gmail.com",
)
