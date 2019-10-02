FROM centos:7.3.1611
RUN echo 'MQTT Bridge is starting'
CMD mqtt_bridge
COPY ./mqtt_bridge /usr/local/bin/mqtt_bridge
