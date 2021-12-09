#bin/spark-class org.apache.spark.deploy.master.Master
cd /web
python manage.py migrate
python manage.py runserver 0.0.0.0:8000
