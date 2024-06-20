docker build -t fact_api .

docker run -p 8000:80 --name fact_api fact_api
http://0.0.0.0:80

docker ps

Ctrl+C or docker stop fact_api
