# challenge-melhor-envio

## Planning
- [x] ETL
- [x] Report
- [x] Generate requirements
- [x] Docs
- [x] PEP8
- [x] Pipeline diagram
- [x] README

## Project description
The challenge is to read a zip file which contains a jsonline data, transforms and load to a MySQL database. Then, extract data from this database and generate a excel file report. All this process run on Docker containers.

## Pipeline diagram
![Pipeline](/docs_img/pipeline.png)
## Instructions do run this project

Run the `run_project.sh` script (sudo ./run_project.sh) or execute the following commands:

- Build python-etl-app image:
`docker build -t python-etl-app ./etl_app`

![Python ETL App output](/docs_img/build_etl_app.png)

- Build mysql-etl-app image
`docker build -t mysql-etl-app ./mysql`

![Mysql output](/docs_img/build_mysql.png)

- Run docker compose
`docker-compose up`

![ETL App execution](/docs_img/etl_app_finished.png)

### Report
Sample of the data generated on `output_report` folder:
![File output sample](/docs_img/file_output_sample.png)
