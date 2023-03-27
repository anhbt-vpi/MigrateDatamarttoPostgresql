# MigrateDatamarttoPostgresql
## Introduction

This is a function written in Python that provides feature read data in Datamart and write data to a local Postgresql database.

## System Requirements

Required: a local Postgresql database

All the requirements for this project have been configured in the Dockerfile which will build a Docker container when you run the project.

## Installation

Before run the app, change info of your local database in the main.py under the comment "# Set your local database here"

To run the app, use the following command in terminal:

`docker-compose up --build`

## Usage

After run the app, data will apear in your local database.
