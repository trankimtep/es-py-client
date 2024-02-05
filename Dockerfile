FROM python:3.8
WORKDIR /es-py-client
COPY ./requirements.txt .
RUN pip3 install -r requirements.txt
RUN apt update && apt install -y systemd 
COPY . . 
CMD ["python3", "main.py"]
