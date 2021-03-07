### Table of Contents
1. [Introduction](README.md#introduction)
1. [The approach in this repository](README.md#The-approach-in-this-repository)
1. [Filtering measurement errors and identifying the three machine states](Filtering-measurement-errors-and-identifying-the-three-machine-states)
1. [Setup using an Amazon EC2 instance](README.md#Setup-using-an-Amazon-EC2-instance)
1. [Next steps](README.md#Next-steps)

### Introduction
Machines can be in one of three states. In a *normal* mode a machine operates as it should and measured data behaves in a predictable way with moderate noise. In the second, *faulty*, state the measured data behaves differently. Finally, in the *failed* state all measurements are close to zero

The purpose:
- Get csv files containing sensor readings from Google Drive, filter out erroneous readings and map the data into a database
- Identify the time epoches at which the machines change state (from *normal* to *faulty* then to *failed*) by analyzing the cleaned data

### The approach in this repository
### Filtering measurement errors and identifying the three machine states 
### Setup using an Amazon EC2 instance
To run the code in this repository on an Amazon EC2, follow these steps.
- In the terraform directory run
    ```terraform
    terraform init
    terraform plan
    terraform apply
    ```
- This will 
    - Launch an EC2 instance with ```timescaledb``` preinstalled in it
    - Clone this repository
    - Setup the necessary python environmnet

- Configure the database as follows
    - Create a database user and grant it a superuser access so that it 
can enable the timescaledb extension
    - create a database and schema with name ```example_co```

- Then follow the instructions on [this](https://developers.google.com/drive/api/v3/quickstart/python) page (only step 1) to enable access to the Google Drive where the csv files are saved.

- Finally, run the python code as follows to setup the database and ingest csv files from Google Drive to the database.
    ```python
    python database_util.py
    python ingress.py
    ```

### Next steps