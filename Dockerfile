FROM python:3.5-alpine
COPY . /code
WORKDIR /code
RUN python setup.py install
RUN pip install gunicorn
ARG PORT="8910"
ENV PORT=$PORT
CMD gunicorn ingress:app -w 9 -t 300 -b 0.0.0.0:${PORT}
