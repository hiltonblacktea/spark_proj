
FROM gettyimages/spark:latest

RUN pip install selenium django djangorestframework findspark 

RUN mkdir -p /web /spark /crawler

COPY  web /web 
COPY  spark /spark 
COPY crawler /crawler
COPY entrypoint.sh /