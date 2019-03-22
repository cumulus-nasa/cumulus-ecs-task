FROM node:8.10

USER root
RUN apt-get update
RUN apt-get install -y unzip

COPY . /home/service

RUN groupadd -r service -g 433
RUN useradd -u 431 -r -g service -d /home/service -s /sbin/nologin -c "Docker image user" service
RUN chown -R service:service /home/service

WORKDIR /home/service

RUN npm install --production

ENTRYPOINT [ "./bin/start.sh" ]
