
FROM gettyimages/spark:latest

RUN pip install selenium django djangorestframework findspark

RUN groupadd -g 1000 seluser
RUN useradd -g seluser -u 1000 seluser 

RUN mkdir -p /web /spark /crawler /tmp/download

COPY web /web 
COPY spark /spark 
COPY crawler /crawler
COPY entrypoint.sh /

RUN chown -R seluser:seluser /web /spark /crawler /tmp/download
