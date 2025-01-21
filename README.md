# Texas Campaign Finance Package

## Overview
This package is designed to provide a simple interface for accessing campaign finance data from the Texas Ethics Commission.
It also reduces duplication of fields and joins data from multiple files into a single table to reduce the size of the data.

## Examples
Across all files, there are over 317 columns. This package reduces the number of columns to [number of columns] by joining data from multiple files.

## Ability to Download TEC File Data Built-In
Using [Selenium](https://www.selenium.dev/), this package can download the latest campaign finance data from the Texas Ethics Commission website. The data is then processed and saved as CSV files.


## Dependencies
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Pydantic](https://img.shields.io/badge/Pydantic-E92063?style=for-the-badge&logo=Pydantic&logoColor=white)
![Pandas](https://img.shields.io/badge/Pandas-2C2D72?style=for-the-badge&logo=pandas&logoColor=white)
![Selenium](https://img.shields.io/badge/Selenium-43B02A?style=for-the-badge&logo=Selenium&logoColor=white)