# Chess\.com EtLT project

This file will serve as our project documentation, as well as providing an walkthrough for instructors on the implemented architecture and execution steps. Feel free to update it whenever needed.

## About

## Data sources

## Design

## Running instructions

This pipeline could be executed in two modes: run module as a script locally and in a Docker container (both building and pulling an image). Here are the running instructions for each option.

<details>
<summary>
 Local execution
</summary>

**Prerequisites**: 
1. You need to have postresql v14 installed on your machine. It should have `postgres` db with a password `postgres`.
2. You need to have conda environment activated to satisfy DE Bootcamp requirements from the first module.

**Steps**:
1. You can run the pipeline by executing `python -m pipelines.Chess` command in your terminal 
2. For local execution (running module as a script) use the `.env` file located within `/app` directory. It has `localhost` reference for postgresql. I.e., you don't need to do any extra step here.
3. You will be able to see both processed data and relevant logs in `postgres.public` schema in your PGAdmin.

</details>

<details>
<summary>
 Docker Container
</summary> 

**Prerequisites**: 
1. You need to have postresql v14 installed on your machine. It should have `postgres` db with a password `postgres`.
2. You also need to have Docker Desktop installed and running.
 

**Steps for building**:
1. From the root directory run command `docker build -t <image_name>:<version>`.
2. For starting a container, use `.env` file from the root. It has correct references for PostreSQL db host. Your terminal command could be:
```bash
docker run --env-file .env --name=<container_name> <image_name>:<version>
```

**Steps for pulling**:
1. You can pull the latest image of this pipeline by executing `docker pull danihello/chess:1.0`.
2. For starting a container, use `.env` file from the root of this repo. It has correct references for PostreSQL db host. Your terminal command could be:
```bash
docker run --env-file .env --name=<container_name> <image_name>:<version>
```


## Lessons Learned

1. Issue wih checking SSL certificate.