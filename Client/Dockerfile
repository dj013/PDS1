FROM python:3
RUN mkdir ClientHost
COPY ./req.py ClientHost/req.py
WORKDIR ClientHost
RUN pip install requests
CMD [ "python3", "req.py" ]
