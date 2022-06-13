FROM python:3
RUN mkdir ServerHost
COPY ./app.py ServerHost/app.py
WORKDIR ServerHost
EXPOSE 8081
RUN pip install flask
CMD ["python3", "app.py"]



