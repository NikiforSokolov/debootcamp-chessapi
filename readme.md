# Chess\.com EtLT project

## About

<div style="overflow: auto;">
    <img src="docs/artifacts/chess_logo.png" alt="Project Image" style="float: right; margin: 0 0 1em 1em; width: 200px; padding-right: 200px;">
    <p>This project is designed to extract data from the Chess.com API, transform it, and load it into a PostgreSQL database. This project is called "EtLT" because we used some in-memory transformations (before the load to db) and transformations within the PostgreSQL to precalculate some analysis.</p>
    <p>The goal of this project was to enable efficient data analysis and discover valuable insights, trends, and statistical information pertaining to chess.com games archive. This is achieved by implementing a modern and robust pipeline that aligns with the principles of data engineering, allowing for the application and enhancement of our acquired skills.</p>
    <p>Code owners of this repository (project) are:</p>
    <ul>
    <li><a href="https://github.com/danihello">Daniel Premisler</a></li>
    <li><a href="https://github.com/yagvendrajoshi">Yagvendra Joshi</a></li>
    <li><a href="https://github.com/NikiforSokolov">Nikifor Sokolov</a></li>
    </ul>
</div>

## Data sources

The main data source for this project is the public API of Chess.com ([official documentation](https://www.chess.com/news/view/published-data-api) and [community guide](https://www.chess.com/clubs/forum/view/guide-unofficial-api-documentation])). We extract various types of data, including game archives, player information, statistics, and more. The API calls/objects we use include:

- `Player/username`
- `Username/stats`
- `Username/games/archives`
- `Username/games/<timestamp>`

Also, there is a one-off data source `eco_codes.csv` which was created manually and contains information about opennings with their codes (used for data enrichment).

## Documentation

This readme serves as the high-level documentation overview. For detailed explanations of individual functions and classes, please refer to the code itself (`docstring` is used for annotating funcations).

On top of it, there are following useful artifacts in this project:
- [Development Guidelines](docs/development_guidelines.md): The standards and principles our group members have agreed on to streamline the development process.
- [Data Dictionary](docs/data_dictionary.md): Detailed descriptions of all data columns and tables used in the project (both sourse and serve objects).

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
</details>


## Lessons Learned

1. Agree with team members on code standards and approaches in the beginning and add more points with time. This will help streamline the development process.
2. Some api's might have problems with SSL certificates. Think twice before starting to use company's laptop.
3. Having rather more smaller increments to make reviews easier for peers.